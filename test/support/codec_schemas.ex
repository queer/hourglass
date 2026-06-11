defmodule Hourglass.Test.CodecSchemas do
  @moduledoc "Fixture schemas for codec/schema tests."

  defmodule Flat do
    @moduledoc false
    use Hourglass.Schema

    embedded_schema do
      field :name, :string
      field :count, :integer
    end
  end

  defmodule Required do
    @moduledoc false
    use Hourglass.Schema

    embedded_schema do
      field :name, :string
    end

    # Override the default changeset to require :name.
    def changeset(struct, params) do
      struct
      |> Ecto.Changeset.cast(params, [:name])
      |> Ecto.Changeset.validate_required([:name])
    end
  end

  defmodule Address do
    @moduledoc false
    use Hourglass.Schema

    embedded_schema do
      field :city, :string
      field :zip, :string
    end
  end

  defmodule Tag do
    @moduledoc false
    use Hourglass.Schema

    embedded_schema do
      field :label, :string
    end
  end

  defmodule Person do
    @moduledoc false
    use Hourglass.Schema

    embedded_schema do
      field :name, :string
      embeds_one :address, Address
      embeds_many :tags, Tag
    end
  end

  defmodule Completed do
    @moduledoc false
    use Hourglass.Schema

    embedded_schema do
      field :result, :string
    end
  end

  defmodule Failed do
    @moduledoc false
    use Hourglass.Schema

    embedded_schema do
      field :reason, :string
    end
  end

  defmodule Outcome do
    @moduledoc false
    use Hourglass.Schema
    import PolymorphicEmbed

    embedded_schema do
      polymorphic_embeds_one :value,
        types: [completed: Completed, failed: Failed],
        on_type_not_found: :raise,
        on_replace: :update
    end

    def changeset(struct, params) do
      struct
      |> Ecto.Changeset.cast(params, [])
      |> PolymorphicEmbed.cast_polymorphic_embed(:value, required: true)
    end
  end
end
