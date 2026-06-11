defmodule Hourglass.SchemaTest do
  use ExUnit.Case, async: true

  alias Hourglass.Test.CodecSchemas.Flat
  alias Hourglass.Test.CodecSchemas.Required

  describe "flat schema cast/1" do
    test "casts a string-keyed map (the JSON-wire shape) into a struct" do
      assert {:ok, %Flat{name: "ann", count: 3}} =
               Flat.cast(%{"name" => "ann", "count" => 3})
    end

    test "casts an atom-keyed map too" do
      assert {:ok, %Flat{name: "ann", count: 3}} =
               Flat.cast(%{name: "ann", count: 3})
    end

    test "coerces a numeric string for an :integer field" do
      assert {:ok, %Flat{count: 3}} = Flat.cast(%{"count" => "3"})
    end

    test "returns {:error, changeset} on a type mismatch" do
      assert {:error, %Ecto.Changeset{valid?: false}} =
               Flat.cast(%{"count" => "not-an-int"})
    end
  end

  describe "dump/1 + JSON round-trip" do
    test "dump -> Jason.encode! -> Jason.decode! -> cast reproduces the struct" do
      original = %Flat{name: "ann", count: 3}

      round_tripped =
        original
        |> Flat.dump()
        |> Jason.encode!()
        |> Jason.decode!()
        |> Flat.cast()

      assert round_tripped == {:ok, original}
    end
  end

  describe "overridden changeset (validation)" do
    test "missing required field -> {:error, changeset}" do
      assert {:error, %Ecto.Changeset{valid?: false} = cs} = Required.cast(%{})
      assert %{name: ["can't be blank"]} = errors_on(cs)
    end

    test "present required field -> {:ok, struct}" do
      assert {:ok, %Required{name: "x"}} = Required.cast(%{"name" => "x"})
    end
  end

  describe "nested embeds" do
    alias Hourglass.Test.CodecSchemas.Address
    alias Hourglass.Test.CodecSchemas.Person
    alias Hourglass.Test.CodecSchemas.Tag

    test "casts embeds_one and embeds_many from string-keyed JSON shape" do
      assert {:ok, person} =
               Person.cast(%{
                 "name" => "ann",
                 "address" => %{"city" => "NYC", "zip" => "10001"},
                 "tags" => [%{"label" => "a"}, %{"label" => "b"}]
               })

      assert %Person{
               name: "ann",
               address: %Address{city: "NYC", zip: "10001"},
               tags: [%Tag{label: "a"}, %Tag{label: "b"}]
             } = person
    end

    test "round-trips a nested struct through JSON" do
      original = %Person{
        name: "ann",
        address: %Address{city: "NYC", zip: "10001"},
        tags: [%Tag{label: "a"}]
      }

      assert {:ok, ^original} =
               original
               |> Person.dump()
               |> Jason.encode!()
               |> Jason.decode!()
               |> Person.cast()
    end
  end

  describe "cast/1 on malformed input" do
    alias Hourglass.Test.CodecSchemas.Flat

    test "non-map input returns {:error, changeset} instead of raising" do
      assert {:error, %Ecto.Changeset{valid?: false}} = Flat.cast("not a map")
      assert {:error, %Ecto.Changeset{valid?: false}} = Flat.cast(nil)
    end
  end

  describe "polymorphic variants" do
    alias Hourglass.Test.CodecSchemas.Completed
    alias Hourglass.Test.CodecSchemas.Failed
    alias Hourglass.Test.CodecSchemas.Outcome

    test "casts the completed variant" do
      assert {:ok, %Outcome{value: %Completed{result: "ok"}}} =
               Outcome.cast(%{"value" => %{"__type__" => "completed", "result" => "ok"}})
    end

    test "casts the failed variant" do
      assert {:ok, %Outcome{value: %Failed{reason: "cycle"}}} =
               Outcome.cast(%{"value" => %{"__type__" => "failed", "reason" => "cycle"}})
    end

    test "round-trips a variant through the real boundary (Outcome.dump + JSON)" do
      original = %Outcome{value: %Failed{reason: "cycle"}}

      assert {:ok, ^original} =
               original
               |> Outcome.dump()
               |> Jason.encode!()
               |> Jason.decode!()
               |> Outcome.cast()
    end
  end

  defp errors_on(changeset) do
    Ecto.Changeset.traverse_errors(changeset, fn {msg, opts} ->
      Regex.replace(~r"%{(\w+)}", msg, fn _match, key ->
        opts
        |> Keyword.get(String.to_existing_atom(key), key)
        |> to_string()
      end)
    end)
  end
end
