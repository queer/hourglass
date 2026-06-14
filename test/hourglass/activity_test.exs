defmodule Hourglass.ActivityTest do
  @moduledoc """
  Tests for the `use Hourglass.Activity` macro: default + custom
  retry policies, allowlist enforcement, and value-range validation.

  Compile-time tests use `Code.compile_string/1` to invoke the macro
  fresh per case so `assert_raise CompileError` catches `__using__/1`'s
  validation. The fixture modules live at module scope so the
  successful-compile cases can introspect `__activity_retry_policy__/0`.
  """

  use ExUnit.Case, async: true

  alias Hourglass.Activity

  defmodule DefaultPolicyActivity do
    use Hourglass.Activity

    @impl Hourglass.Activity.Behaviour
    def execute(args), do: args
  end

  defmodule CustomPolicyActivity do
    use Hourglass.Activity,
      retry: [
        max_attempts: 5,
        initial_interval: 1_000,
        backoff_coefficient: 2.0,
        max_interval: 30_000
      ]

    @impl Hourglass.Activity.Behaviour
    def execute(args), do: args
  end

  defmodule UnlimitedRetriesActivity do
    use Hourglass.Activity, retry: [max_attempts: 0]

    @impl Hourglass.Activity.Behaviour
    def execute(args), do: args
  end

  describe "default_retry_policy/0" do
    test "returns max_attempts: 1 (no retry)" do
      assert Activity.default_retry_policy() == [max_attempts: 1]
    end
  end

  describe "use Hourglass.Activity" do
    test "without retry → default [max_attempts: 1]" do
      assert DefaultPolicyActivity.__activity_retry_policy__() == [max_attempts: 1]
    end

    test "with retry: [...] → returned verbatim" do
      assert CustomPolicyActivity.__activity_retry_policy__() == [
               max_attempts: 5,
               initial_interval: 1_000,
               backoff_coefficient: 2.0,
               max_interval: 30_000
             ]
    end

    test "max_attempts: 0 means unlimited (Temporal spec) → ok" do
      assert UnlimitedRetriesActivity.__activity_retry_policy__() == [max_attempts: 0]
    end

    test "registers @behaviour Hourglass.Activity.Behaviour" do
      behaviours = DefaultPolicyActivity.__info__(:attributes)[:behaviour] || []
      assert Hourglass.Activity.Behaviour in behaviours
    end
  end

  describe "use Hourglass.Activity, input:/output:" do
    defmodule SampleIn do
      use Hourglass.Schema

      embedded_schema do
        field :x, :integer
      end
    end

    defmodule SampleOut do
      use Hourglass.Schema

      embedded_schema do
        field :y, :integer
      end
    end

    defmodule SampleActivity do
      use Hourglass.Activity, input: SampleIn, output: SampleOut, retry: [max_attempts: 3]
      @impl Hourglass.Activity.Behaviour
      def execute(%SampleIn{x: x}), do: %SampleOut{y: x + 1}
    end

    test "stores input/output type accessors" do
      assert SampleActivity.__activity_input_type__() == SampleIn
      assert SampleActivity.__activity_output_type__() == SampleOut
    end

    test "execute/1 is the behaviour callback and runs the body inline" do
      assert SampleActivity.execute(%SampleIn{x: 1}) == %SampleOut{y: 2}
    end

    test "retry policy still resolves via retry:" do
      assert SampleActivity.__activity_retry_policy__()[:max_attempts] == 3
    end

    test "input/output default to :map when omitted" do
      defmodule Bare do
        use Hourglass.Activity
        @impl Hourglass.Activity.Behaviour
        def execute(arg), do: arg
      end

      assert Bare.__activity_input_type__() == :map
      assert Bare.__activity_output_type__() == :map
    end
  end

  describe "heartbeat/0" do
    test "heartbeat/0 is a :ok no-op (no telemetry) outside an activity dispatch" do
      attach_hb()
      assert Hourglass.Activity.heartbeat() == :ok
      refute_receive {:hb, _}, 50
    end

    test "heartbeat/0 inside a dispatch emits telemetry and does not crash when the holder is absent" do
      Process.put({Hourglass.Activity, :info}, %Hourglass.Activity.Info{
        workflow_id: "w", run_id: "r", activity_id: "a", attempt: 1, task_token: "TOK", task_queue: "tq-1"
      })
      attach_hb()
      assert Hourglass.Activity.heartbeat() == :ok
      assert_receive {:hb, %{task_queue: "tq-1"}}
      Process.delete({Hourglass.Activity, :info})
    end

    defp attach_hb do
      ref = {__MODULE__, make_ref()}
      pid = self()
      :telemetry.attach(ref, [:hourglass, :activity, :heartbeat],
        fn _e, _m, meta, _c -> send(pid, {:hb, meta}) end, nil)
      on_exit(fn -> :telemetry.detach(ref) end)
      ref
    end
  end

  describe "compile-time validation" do
    test "rejects :retryable_error_types (classifier owns eligibility)" do
      assert_raise CompileError, ~r/retryable_error_types/, fn ->
        Code.compile_string("""
        defmodule BadActivityRetryableTypes do
          use Hourglass.Activity, retry: [retryable_error_types: ["Timeout"]]
          @impl true
          def execute(_input), do: nil
        end
        """)
      end
    end

    test "rejects :non_retryable_error_types (classifier owns eligibility)" do
      assert_raise CompileError, ~r/non_retryable_error_types/, fn ->
        Code.compile_string("""
        defmodule BadActivityNonRetryableTypes do
          use Hourglass.Activity,
            retry: [non_retryable_error_types: ["KeyError"]]
          @impl true
          def execute(_input), do: nil
        end
        """)
      end
    end

    test "rejects unknown keys" do
      assert_raise CompileError, ~r/unknown retry key :unknown_key/, fn ->
        Code.compile_string("""
        defmodule BadActivityUnknownKey do
          use Hourglass.Activity, retry: [unknown_key: :foo]
          @impl true
          def execute(_input), do: nil
        end
        """)
      end
    end

    test "rejects negative :max_attempts" do
      assert_raise CompileError, ~r/:max_attempts must be a non-negative integer/, fn ->
        Code.compile_string("""
        defmodule BadActivityNegativeAttempts do
          use Hourglass.Activity, retry: [max_attempts: -1]
          @impl true
          def execute(_input), do: nil
        end
        """)
      end
    end

    test "rejects backoff_coefficient < 1.0" do
      assert_raise CompileError, ~r/:backoff_coefficient must be a number >= 1.0/, fn ->
        Code.compile_string("""
        defmodule BadActivityLowBackoff do
          use Hourglass.Activity, retry: [backoff_coefficient: 0.5]
          @impl true
          def execute(_input), do: nil
        end
        """)
      end
    end

    test "rejects negative :initial_interval" do
      assert_raise CompileError, ~r/:initial_interval must be a non-negative integer/, fn ->
        Code.compile_string("""
        defmodule BadActivityNegativeInitial do
          use Hourglass.Activity, retry: [initial_interval: -100]
          @impl true
          def execute(_input), do: nil
        end
        """)
      end
    end

    test "rejects negative :max_interval" do
      assert_raise CompileError, ~r/:max_interval must be a non-negative integer/, fn ->
        Code.compile_string("""
        defmodule BadActivityNegativeMax do
          use Hourglass.Activity, retry: [max_interval: -100]
          @impl true
          def execute(_input), do: nil
        end
        """)
      end
    end

    test "rejects retry that isn't a keyword list" do
      assert_raise CompileError, ~r/must be a keyword list/, fn ->
        Code.compile_string("""
        defmodule BadActivityNotKeyword do
          use Hourglass.Activity, retry: %{max_attempts: 5}
          @impl true
          def execute(_input), do: nil
        end
        """)
      end
    end
  end
end
