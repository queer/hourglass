defmodule Hourglass.Bridge.Error do
  @moduledoc """
  Tagged failure values returned from the bridge layer.

  `:unreachable` is Temporal not reached, or not answering in time, when
  connecting, registering a worker, or calling about a namespace: worth
  trying again. `:namespace_not_found` is a namespace a worker was given,
  or a call names, unknown to Temporal. `:tonic_error` is any other
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
