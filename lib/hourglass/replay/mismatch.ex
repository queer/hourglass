defmodule Hourglass.Replay.Mismatch do
  @moduledoc "Returned from `Hourglass.Replayer.replay_history/2` on workflow nondeterminism."
  defstruct [:detail]
  @type t :: %__MODULE__{detail: String.t()}
end
