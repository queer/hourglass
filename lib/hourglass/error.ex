defmodule Hourglass.Error do
  @moduledoc """
  Higher-level error returned from `Hourglass.*` facade calls.
  Distinct from `Hourglass.Bridge.Error` (low-level wire error);
  facade functions translate Bridge.Error to this type.
  """

  alias Hourglass.Bridge

  defstruct [:reason, :detail]

  @type reason ::
          :unreachable
          | :namespace_unavailable
          | :already_started
          | :not_found
          | :timeout
          | :canceled
          | :rpc_error

  @type t :: %__MODULE__{reason: reason(), detail: term()}

  @spec new(reason(), term()) :: t()
  def new(reason, detail \\ nil), do: %__MODULE__{reason: reason, detail: detail}

  @doc """
  Translates a low-level `Hourglass.Bridge.Error` into this facade error type.
  """
  @spec from_bridge_error(Bridge.Error.t()) :: t()
  def from_bridge_error(%Bridge.Error{kind: :tonic_error, detail: d}) do
    detail = d || ""

    cond do
      String.contains?(detail, "AlreadyExists") or
        String.contains?(detail, "already running") or
          String.contains?(detail, "already exists") ->
        new(:already_started, d)

      String.contains?(detail, "NotFound") or String.contains?(detail, "not found") ->
        new(:not_found, d)

      true ->
        new(:rpc_error, d)
    end
  end

  def from_bridge_error(%Bridge.Error{kind: :shutdown}), do: new(:unreachable, :shutdown)
  def from_bridge_error(%Bridge.Error{} = err), do: new(:rpc_error, err)
end
