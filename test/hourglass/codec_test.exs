defmodule Hourglass.CodecTest.FakeCodec do
  @behaviour Hourglass.Codec
  @impl Hourglass.Codec
  def cast(raw), do: {:ok, {:casted, raw}}
  @impl Hourglass.Codec
  def dump(v), do: {:dumped, v}
end

defmodule Hourglass.CodecTest do
  use ExUnit.Case, async: true

  alias Hourglass.Codec

  describe "scalar cast/2" do
    test ":string accepts a binary" do
      assert Codec.cast(:string, "hi") == {:ok, "hi"}
    end

    test ":string rejects a non-binary" do
      assert {:error, _reason} = Codec.cast(:string, 5)
    end

    test ":integer coerces a numeric binary" do
      assert Codec.cast(:integer, "5") == {:ok, 5}
    end

    test ":integer rejects junk" do
      assert {:error, _reason} = Codec.cast(:integer, "abc")
    end

    test ":boolean coerces" do
      assert Codec.cast(:boolean, "true") == {:ok, true}
    end

    test ":boolean rejects junk" do
      assert {:error, _reason} = Codec.cast(:boolean, "maybe")
    end

    test ":map accepts a map" do
      assert Codec.cast(:map, %{"a" => 1}) == {:ok, %{"a" => 1}}
    end

    test ":map rejects a non-map" do
      assert {:error, _reason} = Codec.cast(:map, "nope")
    end

    test ":any passes through unchanged" do
      assert Codec.cast(:any, %{"x" => [1, 2]}) == {:ok, %{"x" => [1, 2]}}
    end
  end

  describe "scalar dump/2" do
    test "scalars dump to themselves (already JSON-encodable)" do
      assert Codec.dump(:string, "hi") == "hi"
      assert Codec.dump(:integer, 5) == 5
      assert Codec.dump(:map, %{"a" => 1}) == %{"a" => 1}
    end
  end

  describe "module dispatch" do
    test "delegates cast/2 to the module's cast/1" do
      alias Hourglass.CodecTest.FakeCodec

      assert Codec.cast(FakeCodec, %{"a" => 1}) == {:ok, {:casted, %{"a" => 1}}}
      assert Codec.dump(FakeCodec, :x) == {:dumped, :x}
    end
  end

  describe "cast!/2" do
    test "returns the value on success" do
      assert Codec.cast!(:integer, "5") == 5
    end

    test "raises ArgumentError on failure" do
      assert_raise ArgumentError, fn -> Codec.cast!(:integer, "x") end
    end
  end

  describe "dispatcher round-trip (the workflow/activity boundary contract)" do
    alias Hourglass.Test.CodecSchemas.Flat

    test "schema module: cast(dump) round-trips through JSON" do
      original = %Flat{name: "ann", count: 3}

      wire =
        Flat
        |> Hourglass.Codec.dump(original)
        |> Jason.encode!()
        |> Jason.decode!()

      assert Hourglass.Codec.cast(Flat, wire) == {:ok, original}
    end

    test "scalar: cast(dump) round-trips through JSON" do
      wire =
        :integer
        |> Hourglass.Codec.dump(7)
        |> Jason.encode!()
        |> Jason.decode!()

      assert Hourglass.Codec.cast(:integer, wire) == {:ok, 7}
    end
  end
end
