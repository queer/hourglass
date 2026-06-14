# credo:disable-for-this-file Credo.Check.Refactor.ModuleDependencies
defmodule Hourglass.ActivityRunner do
  @moduledoc """
  Runs a single activity task. Spawned by the
  `Hourglass.ActivityExecutor.DynamicSupervisor` as a Task;
  invokes `<activity_module>.handle(name, args)`; classifies the
  outcome through `Hourglass.Activity.RetryClassifier`;
  wraps the result into an `ActivityTaskCompletion` proto; ships back
  via `Hourglass.BridgeHolder.complete_activity_task/2`.

  Activities are plain Elixir — no determinism constraint, no special
  process model. They do NOT hold a raw bridge `reference()`; they
  call the BridgeHolder API with their `task_queue`. If the holder no
  longer has the task queue registered (Worker is shutting down) the
  call returns `{:error, :worker_not_registered}` and is logged but
  not raised — the activity result is lost from this dispatch and
  Core will redeliver via heartbeat / start-to-close timeout.

  ## Failure handling

  Both the `{:error, reason}` return path and the `rescue` clause feed the
  failure value through `Hourglass.Activity.RetryClassifier.classify/2`.
  The classifier's verdict drives `Failure.application_failure_info`:

    * `type` → `ApplicationFailureInfo.type` and `Failure.source`
    * `message` → `Failure.message`
    * `details` → `ApplicationFailureInfo.details` (when non-nil)
    * `non_retryable` → `ApplicationFailureInfo.non_retryable`
      (computed as `classification != :retryable` — `:unclassified` is
      treated as non-retryable in production for safety)

  `Failure.application_failure_info.non_retryable: true` is what tells
  Temporal Server to stop retrying.

  ## Contract violation

  An activity's `handle/2` must return `{:ok, term()} | {:error, term()}`.
  Any other return shape is a `ContractViolation`: non-retryable, with a
  message naming the offending value.

  ## Test-mode strict

  Under `MIX_ENV=test` (or when `:strict_unclassified_activity_failures`
  is configured `true`), an `:unclassified` classification raises an
  `ArgumentError` BEFORE encoding the completion. The error names the
  originating activity and points at the classifier so missing clauses
  surface at the source. In production the strict guard collapses at
  compile time and the runner falls back to the safe non-retryable
  encoding.
  """

  alias Hourglass.Activity.Info
  alias Hourglass.Activity.RetryClassifier
  alias Hourglass.BridgeHolder

  require Logger

  # Compile-time toggle — zero cost in production. The `Mix.env() == :test`
  # default means CI surfaces missing classifier clauses at the originating
  # activity; production prefers the safe :non_retryable fallback.
  @strict_unclassified Application.compile_env(
                         :hourglass,
                         :strict_unclassified_activity_failures,
                         Mix.env() == :test
                       )

  @spec run(
          activity_task :: map(),
          task_queue :: String.t(),
          complete_fn :: (String.t(), binary() -> :ok | {:error, term()})
        ) :: :ok
  def run(
        activity_task,
        task_queue,
        complete_fn \\ &BridgeHolder.complete_activity_task/2
      ) do
    completion =
      try do
        invoke(activity_task, task_queue)
      rescue
        e -> failed_from_exception(activity_task, e)
      catch
        kind, value -> failed_from_caught(activity_task, kind, value)
      end

    completion_bytes = encode_completion(activity_task, completion)

    case complete_fn.(task_queue, completion_bytes) do
      :ok ->
        :ok

      {:error, err} ->
        Logger.error(
          "BridgeHolder.complete_activity_task failed: #{inspect(err)} (task_queue=#{task_queue})"
        )

        :ok
    end
  end

  # An activity body raised: classify the exception, emit telemetry, and build
  # the failed-completion payload.
  defp failed_from_exception(activity_task, exception) do
    context = %{
      activity_name: describe_activity_task(activity_task)[:activity_type] || "",
      caller: :rescue
    }

    {classification, metadata} = RetryClassifier.classify(exception, context)

    maybe_strict_unclassified_exception!(activity_task, exception, classification)
    emit_telemetry_for_exception(activity_task, classification, metadata)

    :telemetry.execute(
      [:hourglass, :activity, :exception],
      %{count: 1},
      Enum.into(describe_activity_task(activity_task), %{
        classification: classification,
        kind: :error
      })
    )

    {:failed,
     %{
       type: metadata.type,
       message: metadata.message,
       details: metadata.details,
       non_retryable: classification != :retryable
     }}
  end

  # `kind` ∈ :throw | :exit | :error. These are not exception structs so the
  # classifier has no clause for them; they're always treated as non-retryable
  # (a process can't recover from a throw/exit it didn't expect, and a retry
  # would just reproduce the same state).
  defp failed_from_caught(activity_task, kind, value) do
    :telemetry.execute(
      [:hourglass, :activity, :exception],
      %{count: 1},
      Enum.into(describe_activity_task(activity_task), %{kind: kind, value: inspect(value)})
    )

    {:failed,
     %{
       type: "Caught.#{kind}",
       message: "#{kind}: #{inspect(value)}",
       details: nil,
       non_retryable: true
     }}
  end

  defp invoke(%{variant: {:cancel, %{reason: reason}}, task_token: task_token}, _task_queue) do
    # Temporal sends a Cancel variant when a workflow is terminated, an activity heartbeat times
    # out, or completion races a cancellation request. Mark the token so the (still-heartbeating)
    # running activity observes the cancel at its next Hourglass.Activity.heartbeat/0 and stops,
    # then acknowledge the cancel back to Core via a Cancellation completion. The Start task's
    # `after` block clears the token once it finishes.
    Hourglass.Activity.CancelRegistry.mark(task_token, reason)

    :telemetry.execute([:hourglass, :activity, :cancel_received], %{count: 1}, %{reason: reason})

    {:cancelled, reason}
  end

  defp invoke(activity_task, task_queue) do
    %{variant: {:start, start}} = activity_task
    raw = decode_args(start.input)

    # Expose per-dispatch context (workflow id, run id, activity id, attempt,
    # task_token, task_queue) to the activity body via Hourglass.Activity.info/0.
    # Cleared on the way out — including the raise path — so a failed activity
    # doesn't leak stale context onto whichever process picks the task up next.
    Process.put(
      {Hourglass.Activity, :info},
      build_info(start, activity_task.task_token, task_queue)
    )

    try do
      case find_activity_module(start.activity_type) do
        {:ok, module} ->
          input = Hourglass.Codec.cast!(module.__activity_input_type__(), raw)
          classify_activity_result(module, input)

        :error ->
          {:failed,
           %{
             type: "ActivityNotFound",
             message: "no activity module handles #{start.activity_type}",
             details: nil,
             non_retryable: true
           }}
      end
    after
      Process.delete({Hourglass.Activity, :info})
    end
  end

  # Build the Info struct from the inbound Coresdk.ActivityTask.Start. The
  # `workflow_execution` field is a Temporal.Api.Common.V1.WorkflowExecution
  # message (workflow_id + run_id). Production Bridge-delivered tasks always
  # populate `workflow_execution` and `activity_id`; if either is missing the
  # task is malformed and we let pattern-match failure surface it loudly
  # rather than silently bucketing every malformed task into a single empty
  # `(workflow_id, run_id, activity_id)` slot — callers that key on these
  # fields for idempotency would mis-correlate every malformed dispatch.
  defp build_info(
         %Coresdk.ActivityTask.Start{
           workflow_execution: %Temporal.Api.Common.V1.WorkflowExecution{
             workflow_id: workflow_id,
             run_id: run_id
           },
           activity_id: activity_id,
           attempt: attempt
         },
         task_token,
         task_queue
       )
       when is_binary(activity_id) do
    %Info{
      workflow_id: workflow_id,
      run_id: run_id,
      activity_id: activity_id,
      attempt: normalise_attempt(attempt),
      task_token: task_token,
      task_queue: task_queue
    }
  end

  # Coresdk.ActivityTask.Start.attempt is `:uint32`; protobuf decodes an
  # unset uint32 as 0. Treat 0 as 1 — a dispatch is by definition an attempt.
  defp normalise_attempt(n) when is_integer(n) and n >= 1, do: n
  defp normalise_attempt(_attempt), do: 1

  defp classify_activity_result(module, input) do
    case module.execute(input) do
      {:error, reason} ->
        context = %{activity_name: inspect(module), caller: :tuple_error}
        {classification, metadata} = RetryClassifier.classify(reason, context)

        maybe_strict_unclassified_return!(module, reason, classification)
        emit_telemetry(module, classification, metadata)

        {:failed,
         %{
           type: metadata.type,
           message: metadata.message,
           details: metadata.details,
           non_retryable: classification != :retryable
         }}

      value ->
        # Bare value (including maps, structs, scalars) → success.
        # Dump via the activity's output type before JSON-encoding.
        dumped = Hourglass.Codec.dump(module.__activity_output_type__(), value)
        {:succeeded, dumped}
    end
  end

  # Resolves an activity_type string to the Elixir module that handles it.
  #
  # Resolution is purely structural: the wire format is `Atom.to_string(module)`
  # (e.g. "Elixir.My.Activity"), so we recover the module atom via
  # `String.to_existing_atom/1`. The recovered module must be a loaded
  # Hourglass activity — verified by the `__activity_input_type__/0` marker
  # injected by `use Hourglass.Activity`.
  #
  # Any activity_type that cannot be parsed (no "Elixir." prefix, unknown atom,
  # etc.) OR that resolves to a module without the marker returns `:error`,
  # which the caller surfaces as a non-retryable `ActivityNotFound` failure.
  defp find_activity_module(activity_type) do
    case parse_activity_module(activity_type) do
      nil ->
        emit_dispatch_failed!(activity_type, nil)

        Logger.error(
          "ActivityRunner: cannot resolve activity_type #{inspect(activity_type)} " <>
            "to a loaded Hourglass activity (no \"Elixir.\" prefix or unknown atom)"
        )

        :error

      mod ->
        if Code.ensure_loaded?(mod) and function_exported?(mod, :__activity_input_type__, 0) do
          {:ok, mod}
        else
          emit_dispatch_failed!(activity_type, mod)

          Logger.error(
            "ActivityRunner: #{inspect(mod)} is not a loaded Hourglass activity " <>
              "(missing __activity_input_type__/0 — did you `use Hourglass.Activity`?)"
          )

          :error
        end
    end
  end

  # Parses an activity_type wire string to an Elixir module atom.
  # The wire format is `Atom.to_string(module)`, i.e. "Elixir.My.Activity".
  # Returns nil for anything that isn't an "Elixir." prefixed string or for
  # atoms that don't exist in this VM.
  defp parse_activity_module(activity_type) do
    case activity_type do
      "Elixir." <> _rest ->
        String.to_existing_atom(activity_type)

      _other ->
        nil
    end
  rescue
    ArgumentError -> nil
  end

  defp decode_args(payloads) do
    case payloads do
      [%{data: data} | _rest] -> Jason.decode!(data)
      [] -> nil
    end
  end

  # ──────────────────────────────────────────────────────────────────────
  # Strict-mode + telemetry
  # ──────────────────────────────────────────────────────────────────────

  if @strict_unclassified do
    defp maybe_strict_unclassified_return!(module, reason, :unclassified) do
      raise ArgumentError, """
      Activity #{inspect(module)}.execute/1 produced an unclassified failure shape:

        #{inspect(reason)}

      Add a clause to Hourglass.Activity.RetryClassifier.classify/2 covering
      this shape (with a unit test).

      To disable this strict check (production default), set
      config :hourglass, :strict_unclassified_activity_failures, false.
      """
    end

    defp maybe_strict_unclassified_return!(_module, _reason, _classification), do: :ok

    defp maybe_strict_unclassified_exception!(activity_task, exception, :unclassified) do
      activity_type =
        case activity_task do
          %{variant: {:start, %{activity_type: t}}} -> t
          _other -> "<unknown>"
        end

      raise ArgumentError, """
      Activity #{activity_type} raised an unclassified exception:

        #{inspect(exception)}

      Add a clause to Hourglass.Activity.RetryClassifier.classify/2 covering
      this exception type (with a unit test).

      To disable this strict check (production default), set
      config :hourglass, :strict_unclassified_activity_failures, false.
      """
    end

    defp maybe_strict_unclassified_exception!(_activity_task, _exception, _classification),
      do: :ok
  else
    defp maybe_strict_unclassified_return!(_module, _reason, _classification), do: :ok

    defp maybe_strict_unclassified_exception!(_activity_task, _exception, _classification),
      do: :ok
  end

  defp emit_telemetry(module, classification, metadata) do
    :telemetry.execute(
      [:hourglass, :activity, :failure],
      %{count: 1},
      %{
        module: module,
        classification: classification,
        type: metadata.type
      }
    )
  end

  defp emit_telemetry_for_exception(activity_task, classification, metadata) do
    activity_type =
      case activity_task do
        %{variant: {:start, %{activity_type: t}}} -> t
        _other -> "<unknown>"
      end

    :telemetry.execute(
      [:hourglass, :activity, :failure],
      %{count: 1},
      %{
        activity_type: activity_type,
        classification: classification,
        type: metadata.type,
        source: :rescue
      }
    )
  end

  # Pull diagnostic fields off the activity task for the rescue/catch
  # telemetry sites. The activity_task arrives as a
  # `Coresdk.ActivityTask.ActivityTask` proto struct in production —
  # bracket access (`task[:activity_type]`) raises
  # `UndefinedFunctionError: Access` because protobuf-generated
  # structs don't implement Access. The actual fields live inside
  # `task.variant = {:start, %Start{…}}`, never on the outer struct.
  # Returns a keyword list ready to splice into telemetry metadata.
  defp describe_activity_task(%{variant: {:start, %{} = start}}) do
    exec = Map.get(start, :workflow_execution)

    [
      activity_type: Map.get(start, :activity_type),
      workflow_id: exec && Map.get(exec, :workflow_id),
      run_id: exec && Map.get(exec, :run_id),
      attempt: Map.get(start, :attempt)
    ]
  end

  defp describe_activity_task(%{variant: {:cancel, %{reason: reason}}}) do
    [
      activity_type: "<cancel>",
      workflow_id: nil,
      run_id: nil,
      attempt: nil,
      cancel_reason: reason
    ]
  end

  defp describe_activity_task(_other) do
    [activity_type: nil, workflow_id: nil, run_id: nil, attempt: nil]
  end

  # ──────────────────────────────────────────────────────────────────────
  # Completion encoding
  # ──────────────────────────────────────────────────────────────────────

  defp encode_completion(activity_task, {:cancelled, reason}) do
    failure = %Temporal.Api.Failure.V1.Failure{
      message: "Activity cancelled (reason=#{inspect(reason)})",
      source: "Hourglass.ActivityRunner",
      failure_info: {:canceled_failure_info, %Temporal.Api.Failure.V1.CanceledFailureInfo{}}
    }

    completion = %Coresdk.ActivityTaskCompletion{
      task_token: activity_task.task_token,
      result: %Coresdk.ActivityResult.ActivityExecutionResult{
        status: {:cancelled, %Coresdk.ActivityResult.Cancellation{failure: failure}}
      }
    }

    Protobuf.encode(completion)
  end

  defp encode_completion(activity_task, {:succeeded, value}) do
    payload = %Temporal.Api.Common.V1.Payload{
      metadata: %{"encoding" => "json/plain"},
      data: Jason.encode!(value)
    }

    completion = %Coresdk.ActivityTaskCompletion{
      task_token: activity_task.task_token,
      result: %Coresdk.ActivityResult.ActivityExecutionResult{
        status: {:completed, %Coresdk.ActivityResult.Success{result: payload}}
      }
    }

    Protobuf.encode(completion)
  end

  defp encode_completion(activity_task, {:failed, failure_data}) do
    %{type: type, message: message, details: details, non_retryable: non_retryable} =
      failure_data

    application_info = %Temporal.Api.Failure.V1.ApplicationFailureInfo{
      type: type,
      non_retryable: non_retryable,
      details: encode_details(details)
    }

    failure = %Temporal.Api.Failure.V1.Failure{
      message: message,
      source: type,
      failure_info: {:application_failure_info, application_info}
    }

    completion = %Coresdk.ActivityTaskCompletion{
      task_token: activity_task.task_token,
      result: %Coresdk.ActivityResult.ActivityExecutionResult{
        status: {:failed, %Coresdk.ActivityResult.Failure{failure: failure}}
      }
    }

    Protobuf.encode(completion)
  end

  # `details` is `ApplicationFailureInfo.details :: Payloads`. We encode a
  # non-nil details map as a single json/plain Payload so downstream consumers
  # (history inspectors, replay tooling) can decode it. Nil → no Payloads at all.
  # Unencodable maps fall back to inspect to preserve at least a textual trail.
  defp encode_details(nil), do: nil

  defp encode_details(details) when is_map(details) do
    data =
      case Jason.encode(details) do
        {:ok, json} -> json
        {:error, _reason} -> Jason.encode!(%{inspect: inspect(details)})
      end

    payload = %Temporal.Api.Common.V1.Payload{
      metadata: %{"encoding" => "json/plain"},
      data: data
    }

    %Temporal.Api.Common.V1.Payloads{payloads: [payload]}
  end

  # Emitted when Temporal hands the worker an activity_type that cannot be
  # resolved to a loaded Hourglass activity module. The non-retryable
  # ActivityNotFound completion is what Core sees; this telemetry event lets
  # hosts project the dispatch failure into their own audit model.
  defp emit_dispatch_failed!(activity_type, target_module) do
    :telemetry.execute(
      [:hourglass, :activity, :dispatch_failed],
      %{count: 1},
      %{
        activity_type: to_string(activity_type),
        target_module: if(is_nil(target_module), do: "", else: inspect(target_module))
      }
    )
  end
end
