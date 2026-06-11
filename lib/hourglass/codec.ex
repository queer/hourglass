defmodule Hourglass.Codec do
  @moduledoc """
  The typed-payload boundary for Hourglass workflow/activity I/O.

  A **type** is either a module implementing this behaviour (typically via
  `use Hourglass.Schema`) or one of the built-in scalar atoms:
  `:string`, `:integer`, `:float`, `:boolean`, `:map`, `:any`.

  `cast/2` turns a JSON-decoded term (string-keyed maps, primitives) into a
  validated value; `dump/2` turns a value into a JSON-encodable term. These
  are the functions the workflow/activity entry wrappers call at the wire
  boundary.
  """

  @type scalar :: :string | :integer | :float | :boolean | :map | :any
  @type type :: module() | scalar()

  @doc "Cast a JSON-decoded term into a validated value, or an error."
  @callback cast(term()) :: {:ok, term()} | {:error, Ecto.Changeset.t()}

  @doc "Serialize a value to a JSON-encodable term."
  @callback dump(term()) :: term()

  @scalars [:string, :integer, :float, :boolean, :map, :any]

  @doc "Cast a JSON-decoded `raw` term against `type`."
  @spec cast(type(), term()) :: {:ok, term()} | {:error, term()}
  def cast(type, raw) when type in @scalars, do: cast_scalar(type, raw)
  def cast(module, raw) when is_atom(module), do: module.cast(raw)

  @doc "Like `cast/2` but returns the value or raises on error."
  @spec cast!(type(), term()) :: term()
  def cast!(type, raw) do
    case cast(type, raw) do
      {:ok, value} ->
        value

      {:error, reason} ->
        raise ArgumentError,
              "Hourglass.Codec.cast!/2 failed for #{inspect(type)}: #{inspect(reason)}"
    end
  end

  @doc "Dump `value` to a JSON-encodable term according to `type`."
  @spec dump(type(), term()) :: term()
  def dump(type, value) when type in @scalars, do: value
  def dump(module, value) when is_atom(module), do: module.dump(value)

  defp cast_scalar(:any, raw), do: {:ok, raw}
  defp cast_scalar(:map, raw) when is_map(raw), do: {:ok, raw}
  defp cast_scalar(:map, raw), do: {:error, {:invalid, :map, raw}}

  defp cast_scalar(type, raw) do
    case Ecto.Type.cast(type, raw) do
      {:ok, value} -> {:ok, value}
      _error -> {:error, {:invalid, type, raw}}
    end
  end
end
