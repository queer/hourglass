defmodule Hourglass.Bridge.Error do
  @moduledoc """
  Tagged failure values returned from the bridge layer.

  `:unreachable` is Temporal not reached, or not answering in time, when
  connecting or registering a worker: worth trying again. `:namespace_not_found`
  is a worker's namespace unknown to Temporal. `:tonic_error` is any other
  failure of a call to Temporal.
  """
  defstruct [:kind, :detail]

  @type kind ::
          :shutdown
          | :tonic_error
          | :unreachable
          | :namespace_not_found
          | :invalid_proto
          | :nondeterminism
          | :worker_already_started
          | :unknown
          | :test

  @type t :: %__MODULE__{kind: kind(), detail: String.t() | nil}
end
