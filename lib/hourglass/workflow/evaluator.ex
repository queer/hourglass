# credo:disable-for-this-file Credo.Check.Refactor.ModuleDependencies
defmodule Hourglass.Workflow.Evaluator do
  @moduledoc """
  Pure recursive evaluator for Hourglass workflows. Per activation:

   1. Replay-walks the activation's jobs to extend a `Workflow.State`:
      `initialize_workflow` → `state.input`; each `resolve_activity{seq}`
      → `state.resolved_results[seq] = decoded_value`; `remove_from_cache`
      → return `:evict` shape.

   2. Re-executes the workflow module's `run/1` inline (no Task spawn).
      The Workflow API primitives consult the evaluator state via the
      process dict (`CommandAccumulator`) and either return a resolved
      value or `throw(:hourglass_temporal_suspend)` once the body hits a
      command that has no result yet.

   3. Catches the suspend sentinel at the `evaluate/3` boundary, sorts the
      accumulated commands by command_id (lexicographic), assigns monotonic
      global seqs starting at `state.next_global_seq`, and encodes a
      `WorkflowActivationCompletion` proto.

  No GenServer. No `:persistent_term`. No `send + receive` suspension.
  State is threaded through arguments; the only mutable handle is the
  process-dict command accumulator + the evaluator-state pointer, both
  cleared on the way out.
  """

  alias Coresdk.WorkflowCommands.CompleteWorkflowExecution
  alias Coresdk.WorkflowCommands.ContinueAsNewWorkflowExecution
  alias Coresdk.WorkflowCommands.FailWorkflowExecution
  alias Coresdk.WorkflowCommands.ScheduleActivity
  alias Coresdk.WorkflowCommands.SetPatchMarker
  alias Coresdk.WorkflowCommands.StartChildWorkflowExecution
  alias Coresdk.WorkflowCommands.StartTimer
  alias Coresdk.WorkflowCommands.WorkflowCommand
  alias Coresdk.WorkflowCompletion.Failure
  alias Coresdk.WorkflowCompletion.Success
  alias Coresdk.WorkflowCompletion.WorkflowActivationCompletion
  alias Hourglass.Workflow.CommandAccumulator
  alias Hourglass.Workflow.State

  require Logger

  @suspend :hourglass_temporal_suspend

  @spec evaluate(
          workflow_module :: module(),
          activation :: map(),
          state :: State.t()
        ) :: {:ok, WorkflowActivationCompletion.t(), State.t()}
  def evaluate(workflow_module, activation, %State{} = state) do
    prior_result = state.result

    # Bind the workflow module to the cached state so subsequent activations
    # (which may not carry `initialize_workflow`) can route via
    # `Hourglass.Worker.WorkflowTypeResolver`, then apply the per-activation reset.
    prepared = reset_per_activation(%{state | workflow_module: workflow_module}, activation)

    {ingest_status, ingested} = ingest_jobs(activation, prepared)

    cond do
      # Once the workflow has previously reached a terminal outcome —
      # completed OR failed (via `fail/2,3`) — every subsequent activation
      # re-emits the cached terminal completion rather than re-deciding.
      # Matches GenServer-runner semantics for :completed; required for
      # :failed too, so a redelivered activation cannot re-run the body and
      # reach a *different* terminal outcome. Temporal Server treats the
      # duplicate as a no-op once it has already observed the terminal state.
      terminal_result?(prior_result) ->
        completion = build_success_completion(ingested, [terminal_command(prior_result)])
        {:ok, completion, %{ingested | result: prior_result}}

      ingest_status == :evict ->
        # Pure cache-eviction activation with no prior terminal state — ack
        # with empty Success and signal eviction in the returned state.
        {:ok, empty_success_completion(ingested), %{ingested | result: :evict}}

      true ->
        run_body(workflow_module, ingested)
    end
  end

  @spec terminal_result?(State.result()) :: boolean()
  defp terminal_result?({:completed, _value}), do: true
  defp terminal_result?({:failed, _failure_data}), do: true
  defp terminal_result?(_other), do: false

  defp terminal_command({:completed, value}), do: build_complete_command(value)
  defp terminal_command({:failed, failure_data}), do: build_fail_command(failure_data)

  # ---------------------------------------------------------------------------
  # Per-activation reset (commands list emptied; resolvers/resolved_results
  # persist across activations per the design).  `result` is reset here only
  # so a fresh body re-execution starts from `nil`; the prior value is
  # captured before reset and consulted in `evaluate/3` above for cached
  # terminal-completion re-emit.
  #
  # `replaying` is per-activation too, which is why it is (re)assigned here
  # rather than carried: Core sets `is_replaying` from where in THIS
  # execution's history the activation sits. Tests build activations as plain
  # maps, some of which predate the field; `== true` maps a missing flag to
  # "not replaying", the shape every activation built before this epic had.
  # ---------------------------------------------------------------------------

  defp reset_per_activation(%State{} = state, activation) do
    %{
      state
      | commands: [],
        result: nil,
        child_count: 0,
        replaying: Map.get(activation, :is_replaying) == true
    }
  end

  # ---------------------------------------------------------------------------
  # Step 1 — ingest_jobs: walk activation.jobs, extending state.
  # ---------------------------------------------------------------------------

  defp ingest_jobs(%{jobs: jobs}, %State{} = state) when is_list(jobs) do
    do_ingest(jobs, state, :ok)
  end

  defp do_ingest([], state, status), do: {status, state}

  defp do_ingest([%{variant: {:initialize_workflow, init}} | rest], state, status) do
    input =
      case init.arguments do
        [first | _rest] -> decode_payload(first)
        _arguments -> nil
      end

    do_ingest(rest, %{state | input: input}, status)
  end

  defp do_ingest([%{variant: {:resolve_activity, resolve}} | rest], state, status) do
    decoded = decode_activity_resolution(resolve.result)
    new_results = Map.put(state.resolved_results, resolve.seq, decoded)
    new_pending = Map.delete(state.pending_resolvers, resolve.seq)

    do_ingest(
      rest,
      %{state | resolved_results: new_results, pending_resolvers: new_pending},
      status
    )
  end

  # A child workflow resolves TWICE for one seq: the start phase lands in
  # `child_starts`, the result phase reuses the `resolved_results` slot (a seq
  # belongs to exactly one command, so there is no collision). If the child
  # fails to start, the start job arrives and NO result job ever does — which is
  # why `peek_child/2` must consult this map rather than waiting on the result.
  defp do_ingest(
         [%{variant: {:resolve_child_workflow_execution_start, resolve}} | rest],
         state,
         status
       ) do
    decoded = decode_child_start(resolve.status)

    do_ingest(
      rest,
      %{state | child_starts: Map.put(state.child_starts, resolve.seq, decoded)},
      status
    )
  end

  defp do_ingest([%{variant: {:resolve_child_workflow_execution, resolve}} | rest], state, status) do
    decoded = decode_child_result(resolve.result)
    new_results = Map.put(state.resolved_results, resolve.seq, decoded)
    new_pending = Map.delete(state.pending_resolvers, resolve.seq)

    do_ingest(
      rest,
      %{state | resolved_results: new_results, pending_resolvers: new_pending},
      status
    )
  end

  defp do_ingest([%{variant: {:remove_from_cache, _details}} | rest], state, _status) do
    # Eviction job: no state mutation, but flag for the caller. Continue
    # walking remaining jobs (Core may bundle additional jobs alongside
    # the eviction in some sequences).
    do_ingest(rest, state, :evict)
  end

  defp do_ingest([%{variant: {:fire_timer, %{seq: seq}}} | rest], state, status) do
    do_ingest(
      rest,
      %{state | resolved_results: Map.put(state.resolved_results, seq, :fired)},
      status
    )
  end

  defp do_ingest([%{variant: {:signal_workflow, sig}} | rest], state, status) do
    decoded =
      case sig.input do
        [first | _rest] -> decode_payload(first)
        _input -> nil
      end

    buf = Map.get(state.signals, sig.signal_name, [])

    # Signals are consumed in arrival order (Nth await_signal reads the Nth
    # buffered payload); the per-name buffer must stay append-ordered.
    # credo:disable-for-next-line Credo.Check.Refactor.AppendSingleItem
    new_buf = buf ++ [decoded]

    do_ingest(
      rest,
      %{state | signals: Map.put(state.signals, sig.signal_name, new_buf)},
      status
    )
  end

  defp do_ingest([%{variant: {:cancel_workflow, _details}} | rest], state, status) do
    do_ingest(rest, %{state | cancel_requested: true}, status)
  end

  # Core sends `notify_has_patch` pre-emptively — with no command of ours
  # preceding it — whenever the history it is replaying records a patch
  # marker, and it sends each id once, on the activation that replays that
  # marker's own WFT. So this ACCUMULATES into the run's state: an activation
  # carrying no such job must not shrink what earlier ones established.
  defp do_ingest([%{variant: {:notify_has_patch, %{patch_id: patch_id}}} | rest], state, status)
       when is_binary(patch_id) and patch_id != "" do
    do_ingest(
      rest,
      %{state | notified_patches: Map.put(state.notified_patches, patch_id, true)},
      status
    )
  end

  # A notify job we recognise but cannot read a patch id out of — an empty id,
  # or a payload shape this SDK does not know. Recording `""` would make a
  # `patched?("")` call answer true for a patch nobody ever declared, so the
  # job is dropped. It is deliberately NOT left to the unhandled-variant
  # clause below, which would report a variant we do in fact handle.
  defp do_ingest([%{variant: {:notify_has_patch, _payload}} | rest], state, status) do
    do_ingest(rest, state, status)
  end

  defp do_ingest([%{variant: {variant_name, _payload}} | rest], state, status) do
    # Unsupported job variants are no-ops at runtime. Emit telemetry +
    # info-log so operators can see when Core sends a variant we don't
    # yet handle (signal, query, update_random_seed, etc).
    #
    # ErrorReporter is intentionally NOT used: it's a `:error|:exit|:throw`
    # surface, and routine SDK protocol messages (e.g. update_random_seed,
    # which Core fires on every subsequent workflow activation) are not
    # errors. Misusing it raised FunctionClauseError on every activation,
    # crashing the evaluator task and freezing the workflow.
    :telemetry.execute(
      [:hourglass, :workflow, :unhandled_job_variant],
      %{count: 1},
      %{variant: variant_name, run_id: state.run_id}
    )

    Logger.info(
      "[Hourglass.Workflow.Evaluator] unhandled job variant #{inspect(variant_name)} (run_id=#{state.run_id}) — no-op"
    )

    do_ingest(rest, state, status)
  end

  # ---------------------------------------------------------------------------
  # Step 2 — re-execute the workflow body inline.
  # ---------------------------------------------------------------------------

  # Four distinct terminal/suspend outcomes, each caught by its own clause and
  # dispatched to its own one-line finisher — the complexity here is the
  # outcome count itself (suspend / continue-as-new / fail / park), not
  # accidental structure to simplify away. Same shape as finish_suspended/1's
  # disable below: real per-branch cost, not a code smell.
  # credo:disable-for-next-line Credo.Check.Refactor.ABCSize
  defp run_body(workflow_module, %State{} = state) do
    CommandAccumulator.init()
    CommandAccumulator.mark_evaluator_active(state)
    CommandAccumulator.seed_patch_answers(state.patch_answers)

    try do
      input = cast_workflow_input(workflow_module.__workflow_input_type__(), state.input)
      result = workflow_module.run(input)
      dumped = Hourglass.Codec.dump(workflow_module.__workflow_output_type__(), result)
      finish_completed(state, dumped)
    catch
      :throw, @suspend ->
        finish_suspended(state)

      :throw, {:hourglass_temporal_continue_as_new, dumped_input} ->
        finish_continue_as_new(state, dumped_input)

      :throw, {:hourglass_temporal_fail, failure_data} ->
        finish_failed(state, failure_data)

      kind, reason when kind in [:error, :exit] ->
        park_workflow(workflow_module, state, kind, reason, __STACKTRACE__)
    after
      CommandAccumulator.unmark_evaluator()
      CommandAccumulator.clear()
    end
  end

  # Park the workflow as a workflow-task failure: emit telemetry, warn, and
  # build the failed completion. The server retries on the next activation.
  defp park_workflow(workflow_module, %State{} = state, kind, reason, stacktrace) do
    metadata = %{
      kind: kind,
      reason: reason,
      workflow_module: workflow_module,
      run_id: state.run_id
    }

    :telemetry.execute([:hourglass, :workflow, :exception], %{count: 1}, metadata)
    :telemetry.execute([:hourglass, :workflow, :task_failed], %{count: 1}, metadata)

    Logger.warning(
      "[Hourglass.Workflow.Evaluator] workflow parked (workflow-task failure) " <>
        "module=#{inspect(workflow_module)} run_id=#{state.run_id}: #{inspect(reason)}"
    )

    finish_task_failed(state, normalise_failure(kind, reason, stacktrace))
  end

  defp normalise_failure(:error, %_struct{} = exception, _stacktrace), do: exception
  defp normalise_failure(:error, reason, _stacktrace), do: reason
  defp normalise_failure(:exit, reason, _stacktrace), do: reason

  @scalars [:string, :integer, :float, :boolean, :map, :any]

  # Cast the raw workflow input only when the declared type is a schema module.
  # Scalar types (:map, :any, etc.) pass the raw value through unchanged.
  defp cast_workflow_input(type, raw) when type in @scalars, do: raw
  defp cast_workflow_input(type, raw) when is_atom(type), do: Hourglass.Codec.cast!(type, raw)

  # ---------------------------------------------------------------------------
  # Step 3 — finish: build a completion proto + updated state.
  # ---------------------------------------------------------------------------

  # A terminal completion ships the run's patch markers ahead of the terminal
  # command and DROPS every other accumulated command. The asymmetry is the
  # point: an unresolved schedule/timer is an operation the body abandoned, so
  # issuing it after deciding to stop would be wrong — but a patch marker is
  # not an operation, it is the record of a decision the body already acted
  # on. Drop it and the history no longer says which branch ran, so a later
  # replay of that history answers `patched?/1` differently from the run it is
  # replaying.
  defp accumulated_patch_markers do
    CommandAccumulator.take_commands()
    |> Enum.sort_by(fn {command_id, _cmd} -> command_id end)
    |> Enum.flat_map(fn
      {_command_id, {:set_patch_marker, %{patch_id: patch_id}}} ->
        [patch_marker_command(patch_id)]

      _other ->
        []
    end)
  end

  defp finish_completed(%State{} = state, return_value) do
    commands = accumulated_patch_markers() ++ [build_complete_command(return_value)]
    completion = build_success_completion(state, commands)
    {:ok, completion, %{state | result: {:completed, return_value}}}
  end

  # The workflow body ended its run via `fail/2,3`: unlike `park_workflow/5`,
  # this builds a *successful* activation completion carrying
  # `fail_workflow_execution` — the command lives in the same
  # `WorkflowCommand.variant` oneof as `complete_workflow_execution`, so from
  # the wire's perspective this is structurally identical to completing, just
  # with a different command. The server reads FailWorkflowExecution as a
  # terminal `WorkflowExecutionFailed`, not a retry signal.
  defp finish_failed(%State{} = state, failure_data) do
    :telemetry.execute(
      [:hourglass, :workflow, :failed],
      %{count: 1},
      %{workflow_module: state.workflow_module, run_id: state.run_id, type: failure_data.type}
    )

    Logger.warning(
      "[Hourglass.Workflow.Evaluator] workflow failed (terminal) " <>
        "module=#{inspect(state.workflow_module)} run_id=#{state.run_id} " <>
        "type=#{failure_data.type}: #{failure_data.message}"
    )

    commands = accumulated_patch_markers() ++ [build_fail_command(failure_data)]
    completion = build_success_completion(state, commands)
    {:ok, completion, %{state | result: {:failed, failure_data}}}
  end

  defp finish_continue_as_new(%State{} = state, dumped_input) do
    args = [
      %Temporal.Api.Common.V1.Payload{
        metadata: %{"encoding" => "json/plain"},
        data: Jason.encode!(dumped_input)
      }
    ]

    cmd = %WorkflowCommand{
      variant:
        {:continue_as_new_workflow_execution,
         %ContinueAsNewWorkflowExecution{
           workflow_type: "",
           task_queue: "",
           arguments: args
         }}
    }

    commands = accumulated_patch_markers() ++ [cmd]
    {:ok, build_success_completion(state, commands), %{state | result: :continue_as_new}}
  end

  defp finish_task_failed(%State{} = state, reason) do
    completion = %WorkflowActivationCompletion{
      run_id: state.run_id,
      status: {:failed, %Failure{failure: encode_failure(reason)}}
    }

    {:ok, completion, state}
  end

  # credo:disable-for-next-line Credo.Check.Refactor.ABCSize
  defp finish_suspended(%State{} = state) do
    accumulated = CommandAccumulator.take_commands()
    sorted = Enum.sort_by(accumulated, fn {command_id, _cmd} -> command_id end)

    {protos, assignments, next_seq} =
      Enum.reduce(sorted, {[], [], state.next_global_seq}, fn
        {command_id, command_term}, {acc_protos, acc_assigns, seq} ->
          new_protos = command_to_proto(seq, command_term, state.task_queue)
          {acc_protos ++ new_protos, [{seq, command_id, command_term} | acc_assigns], seq + 1}
      end)

    # Harvest the run's patch decisions. Only this finisher does: it is the
    # only one the execution continues past. `finish_completed`/`finish_failed`
    # end the run (later activations re-emit the cached terminal command
    # without re-running the body), `finish_continue_as_new` starts a fresh
    # run with fresh history, and `finish_task_failed` parks — Core redelivers
    # the identical activation, which re-decides identically from the state it
    # already had.
    new_state =
      State.register_assignments(
        %{
          state
          | commands: sorted,
            next_global_seq: next_seq,
            patch_answers: CommandAccumulator.patch_answers()
        },
        Enum.reverse(assignments)
      )

    completion = build_success_completion(new_state, protos)
    {:ok, completion, new_state}
  end

  # ---------------------------------------------------------------------------
  # Suspend / resolve hook called from `Hourglass.Workflow` primitives
  # when running under the pure-function evaluator (no runner).
  # ---------------------------------------------------------------------------

  @doc """
  Called by `Workflow.execute_activity` (and friends) when the evaluator is
  active on this process.  Either returns the resolved value (replay path)
  or appends a fresh command and throws the suspend sentinel.
  """
  @spec suspend_or_resolve(State.command_id(), State.command_term()) :: term() | no_return()
  def suspend_or_resolve(command_id, command_term) do
    state =
      CommandAccumulator.evaluator_state() ||
        raise "Workflow.Evaluator.suspend_or_resolve/2 called outside an evaluator"

    case Map.get(state.command_id_to_seq, command_id) do
      nil ->
        # First time we've seen this command_id — append + suspend.
        CommandAccumulator.append_command(command_id, command_term)
        throw(@suspend)

      seq ->
        case Map.fetch(state.resolved_results, seq) do
          {:ok, value} -> value
          :error -> throw(@suspend)
        end
    end
  end

  @doc """
  Non-throwing variant of `suspend_or_resolve/2` for racing primitives
  (e.g. `await_signal/2` with a timeout). First call appends the command;
  returns `:pending` until resolved, then `{:resolved, value}`. Unlike
  `suspend_or_resolve/2` it never throws the suspend sentinel — the caller
  decides whether to suspend after peeking multiple racers.
  """
  @spec peek_command(State.command_id(), State.command_term()) :: :pending | {:resolved, term()}
  def peek_command(command_id, command_term) do
    state =
      CommandAccumulator.evaluator_state() ||
        raise "Workflow.Evaluator.peek_command/2 called outside an evaluator"

    case Map.get(state.command_id_to_seq, command_id) do
      nil ->
        CommandAccumulator.append_command(command_id, command_term)
        :pending

      seq ->
        case Map.fetch(state.resolved_results, seq) do
          {:ok, value} -> {:resolved, value}
          :error -> :pending
        end
    end
  end

  @doc """
  Two-phase peek for a child workflow command. Like `peek_command/2` it appends
  the command on first sight and never throws — the caller decides when to
  suspend. Unlike an activity, a child resolves twice, so this reports the start
  phase and the result phase separately:

    * `:pending` — command issued; the start has not resolved yet.
    * `{:start_failed, cause}` — the child could not be started. **No result
      resolution will ever arrive**, so a caller MUST NOT keep waiting.
    * `{:start_cancelled, failure}` — cancelled during start.
    * `{:started, run_id}` — running; the result has not resolved yet.
    * `{:started, run_id, {:ok, raw} | {:error, failure}}` — result is in.
  """
  @spec peek_child(State.command_id(), State.command_term()) ::
          :pending
          | {:start_failed, term()}
          | {:start_cancelled, term()}
          | {:started, String.t()}
          | {:started, String.t(), {:ok, term()} | {:error, term()}}
  def peek_child(command_id, command_term) do
    state =
      CommandAccumulator.evaluator_state() ||
        raise "Workflow.Evaluator.peek_child/2 called outside an evaluator"

    case Map.get(state.command_id_to_seq, command_id) do
      nil ->
        CommandAccumulator.append_command(command_id, command_term)
        :pending

      seq ->
        peek_child_phases(state, seq)
    end
  end

  defp peek_child_phases(state, seq) do
    case Map.fetch(state.child_starts, seq) do
      :error ->
        :pending

      {:ok, {:started, run_id}} ->
        case Map.fetch(state.resolved_results, seq) do
          {:ok, result} -> {:started, run_id, result}
          :error -> {:started, run_id}
        end

      {:ok, terminal} ->
        terminal
    end
  end

  @doc false
  @spec derive_child_id(String.t(), State.command_id()) :: String.t()
  def derive_child_id(run_id, command_id), do: generate_uuid(run_id, command_id)

  @doc """
  Synchronous primitives (`uuid`, `random`) consume a deterministic
  command_id but never produce a Temporal command and never suspend. They
  short-circuit through this hook.
  """
  @spec synchronous(State.command_id(), State.command_term()) :: term()
  def synchronous(command_id, {:uuid, _opts}) do
    state =
      CommandAccumulator.evaluator_state() ||
        raise "Workflow.Evaluator.synchronous/2 called outside an evaluator"

    generate_uuid(state.run_id, command_id)
  end

  def synchronous(command_id, {:random, %{max: max}}) do
    state =
      CommandAccumulator.evaluator_state() ||
        raise "Workflow.Evaluator.synchronous/2 called outside an evaluator"

    generate_random(state.run_id, command_id, max)
  end

  # ---------------------------------------------------------------------------
  # Completion builders
  # ---------------------------------------------------------------------------

  defp build_success_completion(%State{run_id: run_id}, commands) do
    %WorkflowActivationCompletion{
      run_id: run_id,
      status: {:successful, %Success{commands: commands}}
    }
  end

  defp empty_success_completion(%State{run_id: run_id}) do
    %WorkflowActivationCompletion{
      run_id: run_id,
      status: {:successful, %Success{commands: []}}
    }
  end

  defp patch_marker_command(patch_id) do
    %WorkflowCommand{
      variant: {:set_patch_marker, %SetPatchMarker{patch_id: patch_id, deprecated: false}}
    }
  end

  defp build_complete_command(return_value) do
    %WorkflowCommand{
      variant:
        {:complete_workflow_execution,
         %CompleteWorkflowExecution{result: encode_result_payload(return_value)}}
    }
  end

  # Same shape ActivityRunner uses for an activity's own terminal failure —
  # see `Hourglass.Failure` — so a workflow's `fail/2,3` and an activity's
  # classified failure encode identically on the wire.
  defp build_fail_command(failure_data) do
    %WorkflowCommand{
      variant:
        {:fail_workflow_execution,
         %FailWorkflowExecution{failure: Hourglass.Failure.application_failure(failure_data)}}
    }
  end

  # ---------------------------------------------------------------------------
  # Command → proto translation.
  # ---------------------------------------------------------------------------

  defp command_to_proto(
         seq,
         {:execute_activity, %{module: mod, args: args, options: opts}},
         task_queue
       ) do
    activity_type = Atom.to_string(mod)

    [
      %WorkflowCommand{
        variant: {
          :schedule_activity,
          build_schedule_activity(seq, activity_type, args, opts, task_queue)
        }
      }
    ]
  end

  defp command_to_proto(
         seq,
         {:start_child, %{module: mod, args: args, options: opts, workflow_id: workflow_id}},
         task_queue
       ) do
    [
      %WorkflowCommand{
        variant: {
          :start_child_workflow_execution,
          build_start_child(seq, workflow_id, Atom.to_string(mod), args, opts, task_queue)
        }
      }
    ]
  end

  # `SetPatchMarker` carries no seq — it is not an operation that resolves, so
  # Core identifies it by `patch_id` alone. The global seq the reduce assigns
  # it is still consumed, which keeps the mapping from command_id to seq
  # dense and identical between the original run and its replay.
  #
  # `deprecated: false` is not a placeholder: `deprecate_patch` is a separate
  # primitive this SDK does not offer, so no caller can set it.
  defp command_to_proto(_seq, {:set_patch_marker, %{patch_id: patch_id}}, _task_queue) do
    [patch_marker_command(patch_id)]
  end

  defp command_to_proto(seq, {:start_timer, %{duration_ms: ms}}, _task_queue) do
    [
      %WorkflowCommand{
        variant: {:start_timer, %StartTimer{seq: seq, start_to_fire_timeout: ms_to_duration(ms)}}
      }
    ]
  end

  defp command_to_proto(_seq, unknown, _task_queue) do
    raise "Hourglass.Workflow.Evaluator.command_to_proto/3: " <>
            "unreachable - unknown command term: #{inspect(unknown)}. " <>
            "Add a clause for new command shapes."
  end

  defp build_schedule_activity(seq, activity_type, args, opts, default_task_queue) do
    resolved_task_queue = Keyword.get(opts, :task_queue, default_task_queue)

    %ScheduleActivity{
      seq: seq,
      activity_id: Integer.to_string(seq),
      activity_type: activity_type,
      task_queue: resolved_task_queue,
      arguments: encode_input_payloads(args),
      schedule_to_close_timeout: ms_to_duration(Keyword.get(opts, :schedule_to_close_timeout)),
      start_to_close_timeout: ms_to_duration(Keyword.get(opts, :start_to_close_timeout)),
      heartbeat_timeout: ms_to_duration(Keyword.get(opts, :heartbeat_timeout)),
      retry_policy: build_retry_policy_proto(Keyword.get(opts, :retry_policy))
    }
  end

  # `namespace` is deliberately left "": core passes the field straight through
  # to the server without substituting the parent's, and the reference Rust SDK's
  # ChildWorkflowOptions has no namespace field at all (its into_command never
  # sets one), so "" is what the official SDK ships and the server resolves it to
  # the parent's namespace. `cancellation_type` is left unset: its zero-value is
  # ABANDON and nothing in 0.5.0 cancels a child, so the field is inert.
  defp build_start_child(seq, workflow_id, workflow_type, args, opts, default_task_queue) do
    %StartChildWorkflowExecution{
      seq: seq,
      namespace: "",
      workflow_id: workflow_id,
      workflow_type: workflow_type,
      task_queue: Keyword.get(opts, :task_queue, default_task_queue),
      input: encode_input_payloads(args),
      parent_close_policy:
        parent_close_policy_proto(Keyword.get(opts, :parent_close_policy, :terminate)),
      workflow_id_reuse_policy:
        reuse_policy_proto(Keyword.get(opts, :workflow_id_reuse_policy, :allow_duplicate)),
      workflow_execution_timeout: ms_to_duration(Keyword.get(opts, :workflow_execution_timeout)),
      workflow_run_timeout: ms_to_duration(Keyword.get(opts, :workflow_run_timeout)),
      workflow_task_timeout: ms_to_duration(Keyword.get(opts, :workflow_task_timeout)),
      retry_policy: build_retry_policy_proto(Keyword.get(opts, :retry_policy))
    }
  end

  @parent_close_policies %{
    terminate: :PARENT_CLOSE_POLICY_TERMINATE,
    abandon: :PARENT_CLOSE_POLICY_ABANDON,
    request_cancel: :PARENT_CLOSE_POLICY_REQUEST_CANCEL
  }

  defp parent_close_policy_proto(policy) do
    Map.get(@parent_close_policies, policy) ||
      raise ArgumentError,
            "invalid :parent_close_policy #{inspect(policy)}; " <>
              "expected one of #{inspect(Map.keys(@parent_close_policies))}"
  end

  # Defaults to :allow_duplicate rather than transmitting the zero-value: the
  # reference Rust SDK normalises Unspecified -> AllowDuplicate when building the
  # command instead of letting an unspecified policy reach the server.
  @reuse_policies %{
    allow_duplicate: :WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
    allow_duplicate_failed_only: :WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE_FAILED_ONLY,
    reject_duplicate: :WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
    terminate_if_running: :WORKFLOW_ID_REUSE_POLICY_TERMINATE_IF_RUNNING
  }

  defp reuse_policy_proto(policy) do
    Map.get(@reuse_policies, policy) ||
      raise ArgumentError,
            "invalid :workflow_id_reuse_policy #{inspect(policy)}; " <>
              "expected one of #{inspect(Map.keys(@reuse_policies))}"
  end

  defp encode_input_payloads(nil), do: []

  defp encode_input_payloads(args) do
    {encoding, data} =
      case Jason.encode(args) do
        {:ok, json} -> {"json/plain", json}
        {:error, _reason} -> {"elixir/inspect", inspect(args)}
      end

    [%Temporal.Api.Common.V1.Payload{metadata: %{"encoding" => encoding}, data: data}]
  end

  defp ms_to_duration(nil), do: nil

  defp ms_to_duration(ms) when is_integer(ms) do
    %Google.Protobuf.Duration{seconds: div(ms, 1000), nanos: rem(ms, 1000) * 1_000_000}
  end

  defp build_retry_policy_proto(nil), do: nil

  defp build_retry_policy_proto(policy) when is_list(policy) do
    %Temporal.Api.Common.V1.RetryPolicy{
      maximum_attempts: Keyword.get(policy, :max_attempts, 1),
      initial_interval: ms_to_duration(Keyword.get(policy, :initial_interval, 1_000)),
      backoff_coefficient: Keyword.get(policy, :backoff_coefficient, 2.0) * 1.0,
      maximum_interval: ms_to_duration(Keyword.get(policy, :max_interval, 100_000))
    }
  end

  # ---------------------------------------------------------------------------
  # Payload + failure encoding.
  # ---------------------------------------------------------------------------

  defp encode_result_payload(term) do
    {encoding, data} =
      case Jason.encode(term) do
        {:ok, json} -> {"json/plain", json}
        {:error, _reason} -> {"elixir/inspect", inspect(term)}
      end

    %Temporal.Api.Common.V1.Payload{
      metadata: %{"encoding" => encoding},
      data: data
    }
  end

  defp encode_failure(reason) do
    message =
      case reason do
        msg when is_binary(msg) -> msg
        _other -> inspect(reason)
      end

    %Temporal.Api.Failure.V1.Failure{message: message}
  end

  # ---------------------------------------------------------------------------
  # Activity result decoding.
  # ---------------------------------------------------------------------------

  defp decode_activity_resolution(nil), do: {:error, :no_result}

  defp decode_activity_resolution(%Coresdk.ActivityResult.ActivityResolution{} = resolution) do
    case resolution.status do
      {:completed, %Coresdk.ActivityResult.Success{result: payload}} ->
        {:ok, decode_payload(payload)}

      {:failed, %Coresdk.ActivityResult.Failure{failure: failure}} ->
        {:error, failure}

      {:cancelled, %Coresdk.ActivityResult.Cancellation{failure: failure}} ->
        {:error, {:cancelled, failure}}

      {:backoff, %Coresdk.ActivityResult.DoBackoff{}} ->
        {:error, :backoff}

      nil ->
        {:error, :no_result}
    end
  end

  defp decode_activity_resolution(other) do
    # Unrecognized resolution shape — Core may have introduced a new variant
    # (signal ack, query result, etc.) that we don't yet handle. Return an
    # error so the workflow fails non-retryably with a clear classification
    # rather than silently treating the unknown shape as a successful result.
    {:error, {:unknown_activity_resolution, other}}
  end

  # -- Child workflow resolution decoding --

  defp decode_child_start(
         {:succeeded,
          %Coresdk.WorkflowActivation.ResolveChildWorkflowExecutionStartSuccess{run_id: run_id}}
       ),
       do: {:started, run_id}

  defp decode_child_start(
         {:failed,
          %Coresdk.WorkflowActivation.ResolveChildWorkflowExecutionStartFailure{cause: cause}}
       ),
       do: {:start_failed, cause}

  defp decode_child_start(
         {:cancelled,
          %Coresdk.WorkflowActivation.ResolveChildWorkflowExecutionStartCancelled{
            failure: failure
          }}
       ),
       do: {:start_cancelled, failure}

  defp decode_child_start(other), do: {:start_failed, {:unknown_child_start_resolution, other}}

  defp decode_child_result(%Coresdk.ChildWorkflow.ChildWorkflowResult{status: status}) do
    case status do
      {:completed, %Coresdk.ChildWorkflow.Success{result: payload}} ->
        {:ok, decode_payload(payload)}

      {:failed, %Coresdk.ChildWorkflow.Failure{failure: failure}} ->
        {:error, failure}

      {:cancelled, %Coresdk.ChildWorkflow.Cancellation{failure: failure}} ->
        {:error, {:cancelled, failure}}

      nil ->
        {:error, :no_result}
    end
  end

  defp decode_child_result(other), do: {:error, {:unknown_child_resolution, other}}

  defp decode_payload(%Temporal.Api.Common.V1.Payload{metadata: meta, data: data} = payload) do
    encoding = Map.get(meta || %{}, "encoding", "")

    case encoding do
      "json/plain" ->
        # Decode failure is a production-code bug, not normal operation: an
        # activity claimed json/plain but produced unparseable bytes. Raise so the
        # workflow fails with a real reason rather than receiving a raw proto
        # struct and crashing later with MatchError on a downstream pattern.
        # Note: typed cast (via __activity_output_type__) happens in execute_activity/2,3;
        # decode_payload only decodes the raw JSON term.
        case Jason.decode(data) do
          {:ok, term} -> term
          {:error, reason} -> raise "json/plain payload decode failed: #{inspect(reason)}"
        end

      _encoding ->
        payload
    end
  end

  defp decode_payload(nil), do: nil
  defp decode_payload(other), do: other

  # ---------------------------------------------------------------------------
  # Deterministic primitives (uuid / random).
  # ---------------------------------------------------------------------------

  defp generate_uuid(run_id, command_id) do
    seed = "#{run_id}:#{inspect(command_id)}"

    <<a::32, b::16, c::16, d::16, e::48>> =
      binary_part(:crypto.hash(:sha256, seed), 0, 16)

    formatted =
      :io_lib.format(
        "~8.16.0b-~4.16.0b-~4.16.0b-~4.16.0b-~12.16.0b",
        [a, b, c, d, e]
      )

    IO.iodata_to_binary(formatted)
  end

  defp generate_random(run_id, command_id, max) do
    seed = "#{run_id}:#{inspect(command_id)}"

    <<n::64>> =
      binary_part(:crypto.hash(:sha256, seed), 0, 8)

    rem(n, max)
  end
end
