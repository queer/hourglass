defmodule Hourglass.Bridge.Error do
  @moduledoc "Tagged failure values returned from the bridge layer."
  defstruct [:kind, :detail]

  @type kind ::
          :shutdown
          | :tonic_error
          | :invalid_proto
          | :nondeterminism
          | :worker_already_started
          | :unknown
          | :test

  @type t :: %__MODULE__{kind: kind(), detail: String.t() | nil}
end
