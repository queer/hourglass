defmodule Hourglass.Schema do
  @moduledoc """
  `use Hourglass.Schema` defines a typed Hourglass payload.

  It is the default `Hourglass.Codec` implementation: an Ecto
  `embedded_schema` with a generated all-fields `changeset/2`, `cast/1` +
  `dump/1`.

      defmodule MyApp.Agent.Goal do
        use Hourglass.Schema

        embedded_schema do
          field :text, :string
          field :turns, :integer
        end
      end

  `cast/1` accepts string- or atom-keyed maps (the JSON-wire shape) and
  returns `{:ok, struct} | {:error, %Ecto.Changeset{}}`. Non-map input
  returns `{:error, changeset}` instead of raising. Override `changeset/2`
  to add validations (e.g. `validate_required/2`); the default casts all
  scalar fields and all embeds.

  `dump/1` returns a JSON-encodable map via `Ecto.embedded_dump/2`, which
  recurses through all embeds and invokes each embed type's dump (including
  `polymorphic_embed`'s type dump that injects the `__type__` discriminator).
  """

  defmacro __using__(_opts) do
    quote do
      use Ecto.Schema
      import Ecto.Changeset
      @behaviour Hourglass.Codec
      @primary_key false
      @before_compile Hourglass.Schema
    end
  end

  defmacro __before_compile__(_env) do
    quote do
      @doc """
      Default all-fields changeset. Override to add validations; call
      `Ecto.Changeset.cast/3` + `cast_embed/2` (or
      `PolymorphicEmbed.cast_polymorphic_embed/2`) yourself when you do.
      """
      def changeset(struct, params), do: Hourglass.Schema.__default_changeset__(struct, params)

      defoverridable changeset: 2

      @impl Hourglass.Codec
      def cast(params), do: Hourglass.Schema.__default_cast__(__MODULE__, params)

      @impl Hourglass.Codec
      def dump(%__MODULE__{} = value), do: Ecto.embedded_dump(value, :json)

      defoverridable cast: 1, dump: 1
    end
  end

  @doc false
  @spec __default_changeset__(struct(), map()) :: Ecto.Changeset.t()
  def __default_changeset__(struct, params) do
    embeds = struct.__struct__.__schema__(:embeds)
    scalar_fields = struct.__struct__.__schema__(:fields) -- embeds

    struct
    |> Ecto.Changeset.cast(params, scalar_fields)
    |> __cast_embeds__(embeds)
  end

  @doc false
  @spec __default_cast__(module(), term()) :: {:ok, struct()} | {:error, Ecto.Changeset.t()}
  def __default_cast__(module, params) when is_map(params) do
    module
    |> struct()
    |> module.changeset(params)
    |> Ecto.Changeset.apply_action(:insert)
  end

  def __default_cast__(module, other) do
    {:error,
     module
     |> struct()
     |> Ecto.Changeset.change()
     |> Ecto.Changeset.add_error(:base, "expected a map, got: #{inspect(other)}")}
  end

  @doc false
  @spec __cast_embeds__(Ecto.Changeset.t(), [atom()]) :: Ecto.Changeset.t()
  def __cast_embeds__(changeset, embeds) do
    Enum.reduce(embeds, changeset, fn embed, cs ->
      Ecto.Changeset.cast_embed(cs, embed)
    end)
  end
end
