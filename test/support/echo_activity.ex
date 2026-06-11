defmodule Hourglass.TestSupport.EchoActivity do
  @moduledoc false
  use Hourglass.Activity, input: :map, output: :map

  @impl Hourglass.Activity.Behaviour
  def execute(%{"nonce" => nonce}), do: %{"nonce" => nonce}
  def execute(%{nonce: nonce}), do: %{"nonce" => nonce}
end
