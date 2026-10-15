defmodule Hourglass.Error do
  @moduledoc """
  Higher-level error returned from `Hourglass.*` facade calls and
  `Hourglass.Client`. Distinct from `Hourglass.Bridge.Error` (low-level
  wire error); facade functions translate Bridge.Error to this type.

  The reasons:

    * `:unreachable` — Temporal was not reached, or did not answer in
      time; trying again may succeed.
    * `:already_started` — a workflow with that id is already running
      (or, as its reuse policy says, may not be started again).
    * `:namespace_exists` — the namespace a register names exists already.
    * `:not_found` — the workflow, or the namespace, is unknown to
      Temporal (or a workflow has closed, for a call that needs it open).
    * `:namespace_unavailable`, `:timeout`, `:canceled` — as named.
    * `:rpc_error` — any other failure; `detail` says what.
  """

  alias Hourglass.Bridge

  defstruct [:reason, :detail]

  @type reason ::
          :unreachable
          | :namespace_unavailable
          | :already_started
          | :namespace_exists
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
  def from_bridge_error(%Bridge.Error{kind: :unreachable, detail: d}), do: new(:unreachable, d)

  def from_bridge_error(%Bridge.Error{kind: :namespace_not_found, detail: d}),
    do: new(:not_found, d)

  def from_bridge_error(%Bridge.Error{kind: :namespace_exists, detail: d}),
    do: new(:namespace_exists, d)

  def from_bridge_error(%Bridge.Error{} = err), do: new(:rpc_error, err)
end
