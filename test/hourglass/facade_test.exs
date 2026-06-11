defmodule Hourglass.FacadeTest do
  @moduledoc """
  Mock-backed tests for the Hourglass facade:
  `start/3`, `signal/3`, `cancel/2`, `status/1`, `history/1`, `result/2`.

  Covers:
    * `start/3` happy-path — input cast+validated, backend receives dumped input.
    * `start/3` invalid input — never reaches backend; returns `{:error, changeset}`.
    * `signal/3` with bare id — normalises to `%WorkflowHandle{}` before dispatch.
    * `cancel/2` with handle — passes handle directly to backend.
    * `status/1` with bare id — normalises to `%WorkflowHandle{}` and builds `%WorkflowStatus{}`.
  """

  use Hourglass.Test.HourglassCase, async: true

  alias Hourglass.WorkflowHandle
  alias Hourglass.WorkflowStatus

  # ---------------------------------------------------------------------------
  # Inline workflow fixtures
  # ---------------------------------------------------------------------------

  defmodule MapWorkflow do
    @moduledoc false
    use Hourglass.Workflow, input: :map, output: :map

    @impl Hourglass.Workflow.Behaviour
    def run(_args), do: %{}
  end

  defmodule RequiredSchema do
    @moduledoc false
    use Hourglass.Schema

    embedded_schema do
      field :name, :string
    end

    def changeset(struct, params) do
      struct
      |> Ecto.Changeset.cast(params, [:name])
      |> Ecto.Changeset.validate_required([:name])
    end
  end

  defmodule SchemaWorkflow do
    @moduledoc false
    use Hourglass.Workflow, input: RequiredSchema, output: :map

    @impl Hourglass.Workflow.Behaviour
    def run(_args), do: %{}
  end

  # ---------------------------------------------------------------------------
  # start/3 — happy path
  # ---------------------------------------------------------------------------

  test "start/3 validates and passes dumped input to backend, returns WorkflowHandle" do
    Mox.expect(TemporalMock, :start_workflow, fn module, dumped, opts ->
      assert module == MapWorkflow
      assert dumped == %{"n" => 1}
      assert opts[:workflow_id] == "w1"
      {:ok, %WorkflowHandle{id: "w1", run_id: "r1"}}
    end)

    assert {:ok, %WorkflowHandle{id: "w1"}} =
             Hourglass.start(MapWorkflow, %{"n" => 1}, id: "w1")
  end

  # ---------------------------------------------------------------------------
  # start/3 — invalid input never reaches backend
  # ---------------------------------------------------------------------------

  test "start/3 returns {:error, changeset} for invalid input and never calls backend" do
    # No expect — Mox.verify! (via setup :verify_on_exit!) will catch any
    # unexpected call to :start_workflow.

    assert {:error, %Ecto.Changeset{valid?: false}} =
             Hourglass.start(SchemaWorkflow, %{}, id: "w2")
  end

  # ---------------------------------------------------------------------------
  # signal/3 — bare id normalised to WorkflowHandle
  # ---------------------------------------------------------------------------

  test "signal/3 with bare id normalises to WorkflowHandle and delegates to backend" do
    Mox.expect(TemporalMock, :signal_workflow, fn handle, name, payload ->
      assert handle == %WorkflowHandle{id: "w1", run_id: ""}
      assert name == "go"
      assert payload == %{"v" => 1}
      :ok
    end)

    assert :ok = Hourglass.signal("w1", "go", %{"v" => 1})
  end

  # ---------------------------------------------------------------------------
  # cancel/2 — handle passthrough
  # ---------------------------------------------------------------------------

  test "cancel/2 with WorkflowHandle passes handle directly to backend" do
    handle = %WorkflowHandle{id: "w1", run_id: ""}

    Mox.expect(TemporalMock, :cancel_workflow, fn h, reason ->
      assert h == %WorkflowHandle{id: "w1", run_id: ""}
      assert reason == "stop"
      :ok
    end)

    assert :ok = Hourglass.cancel(handle, "stop")
  end

  # ---------------------------------------------------------------------------
  # status/1 — bare id normalised to WorkflowHandle
  # ---------------------------------------------------------------------------

  test "status/1 with bare id normalises and returns {:ok, %WorkflowStatus{}}" do
    stub_running(%WorkflowHandle{id: "w1", run_id: ""})

    assert {:ok, %WorkflowStatus{state: :running}} = Hourglass.status("w1")
  end
end
