defmodule Hourglass.Activity.HeartbeatRef do
  @moduledoc """
  A running activity's leave for another process to heartbeat on its
  behalf. Captured inside the activity with
  `Hourglass.Activity.heartbeat_ref/0`, and used from any process with
  `Hourglass.Activity.heartbeat/1`.

  It names one dispatch of one activity — one attempt — and the process
  running it. Treat it as opaque: its fields are not part of the API.
  """

  @enforce_keys [:info, :owner]
  defstruct [:info, :owner]

  @typedoc "Opaque: build it with `Hourglass.Activity.heartbeat_ref/0` only."
  @type t :: %__MODULE__{info: Hourglass.Activity.Info.t(), owner: pid()}
end
