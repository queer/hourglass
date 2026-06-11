defmodule Hourglass.Workflow.Info do
  @moduledoc """
  Per-activation context the evaluator exposes via
  `Hourglass.Workflow.info/0`.

  Built lazily from the active `Hourglass.Workflow.State` (set in
  the evaluator's process dict by `mark_evaluator_active/1`) when a
  workflow body calls `Workflow.info/0`. Cleared by the evaluator's
  `unmark_evaluator/0` on exit, regardless of completion shape.

  The `task_queue` is the queue the workflow itself is running on. The
  v3 source-ingest workflow threads this through to the
  `parse_and_emit_source` / `chunk_and_emit_markdown_bytes` activities
  so that the `:workflow_start` outbox entries those activities append
  for child workflows carry the same queue, keeping all
  per-source per-chunk workflows on the same worker.
  """

  @enforce_keys [:run_id, :task_queue]
  defstruct [:run_id, :task_queue]

  @type t :: %__MODULE__{
          run_id: String.t() | nil,
          task_queue: String.t()
        }
end
