defmodule Hourglass.ActivityRunnerTest do
  # async: false — no Temporal cluster, no DB, but several tests swap the
  # globally-configured classifier via Application.put_env (restored via
  # on_exit). That global mutation would race other async test modules that
  # read `:hourglass, :retry_classifier` (e.g. RetryClassifier.DefaultTest),
  # so this module runs serially.
  use ExUnit.Case, async: false

  alias Hourglass.ActivityRunner
  alias Temporal.Api.Failure.V1.ApplicationFailureInfo
  alias Temporal.Api.Failure.V1.Failure

  # Test-only classifier with NotFound/RateLimited/unclassified semantics.
  # The shipped Default classifier is permissive
  # (everything :retryable), so these shape-specific verdicts and the
  # strict-unclassified raise path need a classifier that actually
  # distinguishes shapes. Installed per-describe via Application.put_env.
  defmodule TestClassifier do
    @behaviour Hourglass.Activity.RetryClassifier

    @impl Hourglass.Activity.RetryClassifier
    def classify(:not_found, _context) do
      {:non_retryable, %{type: "NotFound", message: "not found: :not_found", details: nil}}
    end

    def classify(:rate_limited, _context) do
      {:retryable, %{type: "RateLimited", message: "rate limited", details: nil}}
    end

    def classify(error, _context) when is_exception(error) do
      {:non_retryable,
       %{
         type:
           error.__struct__
           |> Module.split()
           |> List.last(),
         message: Exception.message(error),
         details: nil
       }}
    end

    # Anything else is unclassified — exercises the strict-raise path.
    def classify(reason, _context) do
      {:unclassified, %{type: "Unclassified", message: inspect(reason), details: nil}}
    end
  end

  defp put_classifier(module) do
    prev = Application.get_env(:hourglass, :retry_classifier)
    Application.put_env(:hourglass, :retry_classifier, module)

    on_exit(fn ->
      if is_nil(prev) do
        Application.delete_env(:hourglass, :retry_classifier)
      else
        Application.put_env(:hourglass, :retry_classifier, prev)
      end
    end)
  end

  defmodule EchoActivity do
    use Hourglass.Activity, input: :map, output: :map

    @impl Hourglass.Activity.Behaviour
    # Classified errors: :not_found is :non_retryable, :rate_limited is :retryable.
    def execute(%{"action" => "fail_non_retryable"}), do: {:error, :not_found}
    def execute(%{"action" => "fail_retryable"}), do: {:error, :rate_limited}
    # Programming bug → classifier marks :non_retryable.
    def execute(%{"action" => "crash"}), do: raise(ArgumentError, "kaboom")
    # `throw` is a non-exception kind; it skips the classifier and lands in
    # the runner's `catch kind, value` clause.
    def execute(%{"action" => "throw_oops"}), do: throw(:oops)
    # Default: echo the args back.
    def execute(args), do: args
  end

  defmodule UnclassifiedActivity do
    use Hourglass.Activity, input: :map, output: :map

    # Bare string isn't covered by TestClassifier's shape clauses → :unclassified.
    @impl Hourglass.Activity.Behaviour
    def execute(%{"action" => "fail_unclassified"}), do: {:error, "no clause for me"}
  end

  # A tiny schema for the output-dump test.
  defmodule OutputSchema do
    use Hourglass.Schema

    embedded_schema do
      field :value, :string
    end
  end

  defmodule SchemaOutputActivity do
    use Hourglass.Activity, input: :map, output: Hourglass.ActivityRunnerTest.OutputSchema

    @impl Hourglass.Activity.Behaviour
    def execute(%{"value" => v}), do: %Hourglass.ActivityRunnerTest.OutputSchema{value: v}
  end

  # Build an activity_type string in the new wire format —
  # Atom.to_string(module), e.g. "Elixir.My.Activity".
  defp activity_type(mod), do: Atom.to_string(mod)

  # Wrapping in the proto struct (NOT a plain map) is intentional — the
  # production Bridge decodes wire bytes into
  # `%Coresdk.ActivityTask.ActivityTask{}`, which does NOT implement
  # the Access protocol. A prior version of the rescue/catch path used
  # bracket access (`task[:activity_type]`); it passed under a plain
  # map (Access works on maps) and crashed in prod under the struct.
  # Keeping the fixture struct-shaped reproduces that.
  defp task(activity_type, input_data) when is_binary(activity_type) do
    %Coresdk.ActivityTask.ActivityTask{
      task_token: <<1, 2, 3>>,
      variant:
        {:start,
         %Coresdk.ActivityTask.Start{
           activity_type: activity_type,
           activity_id: "test-activity",
           attempt: 1,
           workflow_execution: %Temporal.Api.Common.V1.WorkflowExecution{
             workflow_id: "test-wf",
             run_id: "test-run"
           },
           input: [
             %Temporal.Api.Common.V1.Payload{
               metadata: %{"encoding" => "json/plain"},
               data: Jason.encode!(input_data)
             }
           ]
         }}
    }
  end

  defp task_for(module, input_data), do: task(activity_type(module), input_data)

  defp run_and_capture(activity_task) do
    parent = self()

    capture = fn _worker, bytes ->
      send(parent, {:captured, bytes})
      :ok
    end

    # ActivityRunner is exercised against many failure paths in this
    # file (unresolvable-activity-type, not-a-hourglass-activity, etc.).
    # Each path Logger.error()s by design — those logs document production-time
    # misconfiguration. Capture them so the test runner output stays clean;
    # tests that need to assert on the log message wrap the call themselves.
    ExUnit.CaptureLog.capture_log(fn ->
      ActivityRunner.run(activity_task, "fake-task-queue", capture)
    end)

    assert_receive {:captured, bytes}
    Coresdk.ActivityTaskCompletion.decode(bytes)
  end

  defp failure_info(completion) do
    {:failed, %{failure: %Failure{} = failure}} = completion.result.status
    {:application_failure_info, %ApplicationFailureInfo{} = info} = failure.failure_info
    {failure, info}
  end

  test "echo activity → completion proto with success" do
    completion = run_and_capture(task_for(EchoActivity, %{"message" => "hello"}))

    assert completion.task_token == <<1, 2, 3>>
    assert {:completed, %{result: payload}} = completion.result.status
    assert Jason.decode!(payload.data) == %{"message" => "hello"}
  end

  test "EchoActivity (TestSupport) echoes nonce via execute/1" do
    # credo:disable-for-next-line Credo.Check.Readability.AliasAs
    alias Hourglass.TestSupport.EchoActivity, as: SupportEcho

    completion = run_and_capture(task_for(SupportEcho, %{"nonce" => "n1"}))

    assert {:completed, %{result: payload}} = completion.result.status
    assert Jason.decode!(payload.data) == %{"nonce" => "n1"}
  end

  test "activity with schema output type: struct result is dumped to json/plain map" do
    completion = run_and_capture(task_for(SchemaOutputActivity, %{"value" => "hello"}))

    assert {:completed, %{result: payload}} = completion.result.status
    decoded = Jason.decode!(payload.data)
    assert decoded["value"] == "hello"
  end

  describe "classified failure shapes (custom classifier)" do
    setup do
      put_classifier(TestClassifier)
      :ok
    end

    test "{:error, :not_found} → non_retryable=true via classifier" do
      completion = run_and_capture(task_for(EchoActivity, %{"action" => "fail_non_retryable"}))

      {failure, info} = failure_info(completion)
      assert info.non_retryable == true
      assert info.type == "NotFound"
      assert failure.source == "NotFound"
      assert failure.message =~ ":not_found"
    end

    test "{:error, :rate_limited} → non_retryable=false via classifier" do
      completion = run_and_capture(task_for(EchoActivity, %{"action" => "fail_retryable"}))

      {_failure, info} = failure_info(completion)
      assert info.non_retryable == false
      assert info.type == "RateLimited"
    end

    test "raised exception → non_retryable=true via classifier" do
      completion = run_and_capture(task_for(EchoActivity, %{"action" => "crash"}))

      {failure, info} = failure_info(completion)
      assert info.non_retryable == true
      assert info.type == "ArgumentError"
      assert failure.message =~ "kaboom"
    end
  end

  # Regression: an earlier rescue/catch tried to extract metadata for the
  # error reporter via `task[:activity_type]` etc. That bracket access raised
  # `Coresdk.ActivityTask.ActivityTask.fetch/2 is undefined` in production
  # because protobuf structs don't implement Access — masking the real
  # activity exception with a runner crash. This test (paired with the
  # struct fixture above) pins the fix: a `throw` lands in the runner's
  # `catch kind, value` branch, which used the same broken bracket access,
  # so this exercises the catch-path describe_activity_task helper.
  test "throw inside activity → handled cleanly (catch path doesn't crash on struct task)" do
    completion = run_and_capture(task_for(EchoActivity, %{"action" => "throw_oops"}))

    {_failure, info} = failure_info(completion)
    assert info.non_retryable == true
    assert info.type == "Caught.throw"
  end

  test "cancel variant → cancelled completion (no MatchError crash)" do
    # Temporal sends a Cancel ActivityTask when a workflow is terminated or
    # an activity races completion. The runner used to MatchError on this
    # because invoke/1 only handled the :start variant; this test pins the
    # acknowledgement path so a future regression resurfaces immediately.
    cancel_task = %Coresdk.ActivityTask.ActivityTask{
      task_token: <<7, 7, 7>>,
      variant: {:cancel, %Coresdk.ActivityTask.Cancel{reason: 1, details: nil}}
    }

    completion = run_and_capture(cancel_task)

    assert completion.task_token == <<7, 7, 7>>

    assert {:cancelled, %Coresdk.ActivityResult.Cancellation{failure: failure}} =
             completion.result.status

    assert {:canceled_failure_info, %Temporal.Api.Failure.V1.CanceledFailureInfo{}} =
             failure.failure_info
  end

  test "activity_type that is not a loaded Hourglass activity → ActivityNotFound" do
    # "Elixir.Hourglass.ActivityRunnerTest.NotRegistered" is an atom that
    # doesn't exist in this VM — structural resolution returns nil → :error.
    bogus_type = "Elixir.Hourglass.ActivityRunnerTest.NotRegistered"
    completion = run_and_capture(task(bogus_type, %{}))

    {failure, info} = failure_info(completion)
    assert info.non_retryable == true
    assert info.type == "ActivityNotFound"
    assert failure.message =~ "no activity module handles"
  end

  # Define a plain module that does NOT use Hourglass.Activity (no marker).
  defmodule NotAnActivity do
    def something, do: :ok
  end

  test "module without __activity_input_type__/0 marker → ActivityNotFound" do
    # The module exists and is loaded, but it's not a Hourglass activity.
    type = Atom.to_string(NotAnActivity)
    completion = run_and_capture(task(type, %{}))

    {failure, info} = failure_info(completion)
    assert info.non_retryable == true
    assert info.type == "ActivityNotFound"
    assert failure.message =~ "no activity module handles"
  end

  test "unparseable activity_type (bare name, no Elixir. prefix) → ActivityNotFound" do
    # Bare name with no module prefix can't be parsed into a module atom.
    completion = run_and_capture(task("echo", %{}))

    {_failure, info} = failure_info(completion)
    assert info.non_retryable == true
    assert info.type == "ActivityNotFound"
  end

  test "dispatch failure emits [:hourglass, :activity, :dispatch_failed] telemetry" do
    # The runner emits telemetry instead of raising. An unknown/unparseable
    # activity_type triggers emit_dispatch_failed!.
    :telemetry_test.attach_event_handlers(self(), [[:hourglass, :activity, :dispatch_failed]])

    bogus_type = "Elixir.Hourglass.ActivityRunnerTest.NotRegistered"
    _completion = run_and_capture(task(bogus_type, %{}))

    assert_receive {[:hourglass, :activity, :dispatch_failed], _ref, %{count: 1},
                    %{activity_type: at}}

    assert at == bogus_type
  end

  describe "test-mode strict (custom classifier produces :unclassified)" do
    setup do
      put_classifier(TestClassifier)
      :ok
    end

    test "unclassified return raises ArgumentError, surfacing as non-retryable completion" do
      # The runner is invoked via try/rescue inside Task spawning in production,
      # but here we call run/4 directly. ActivityRunner.run wraps invoke/2 in a
      # try/rescue — so the strict-mode ArgumentError raised inside
      # classify_activity_result is caught by the rescue clause and
      # re-classified (ArgumentError → :non_retryable per TestClassifier). The
      # completion that ships will therefore be a non-retryable ArgumentError
      # completion whose message references the missing classifier clause.
      parent = self()

      capture = fn _worker, bytes ->
        send(parent, {:captured, bytes})
        :ok
      end

      activity_task = task_for(UnclassifiedActivity, %{"action" => "fail_unclassified"})

      ExUnit.CaptureLog.capture_log(fn ->
        ActivityRunner.run(activity_task, "fake-task-queue", capture)
      end)

      assert_receive {:captured, bytes}
      completion = Coresdk.ActivityTaskCompletion.decode(bytes)
      {failure, info} = failure_info(completion)
      assert info.non_retryable == true
      # Message must point at the classifier so an operator triaging the
      # failure knows where to add the missing clause.
      assert failure.message =~ "RetryClassifier"
      assert failure.message =~ "UnclassifiedActivity"
    end
  end

  describe "telemetry on [:hourglass, :activity, :failure] (custom classifier)" do
    setup do
      put_classifier(TestClassifier)
      :ok
    end

    test "telemetry event fires for classified failure" do
      event = [:hourglass, :activity, :failure]
      :telemetry_test.attach_event_handlers(self(), [event])

      _completion = run_and_capture(task_for(EchoActivity, %{"action" => "fail_non_retryable"}))

      assert_receive {^event, _ref, %{count: 1}, metadata}
      assert metadata.classification == :non_retryable
      assert metadata.type == "NotFound"
      assert metadata.module == EchoActivity
    end

    test "telemetry event fires for rescued exception" do
      event = [:hourglass, :activity, :failure]
      :telemetry_test.attach_event_handlers(self(), [event])

      _completion = run_and_capture(task_for(EchoActivity, %{"action" => "crash"}))

      assert_receive {^event, _ref, %{count: 1}, metadata}
      assert metadata.classification == :non_retryable
      assert metadata.type == "ArgumentError"
      assert metadata.source == :rescue
    end
  end
end
