defmodule Hourglass.Activity.RetryClassifier.DefaultTest do
  use ExUnit.Case, async: true
  alias Hourglass.Activity.RetryClassifier
  alias Hourglass.Activity.RetryClassifier.Default

  test "exceptions classify as retryable with the exception's type name" do
    error = %RuntimeError{message: "boom"}

    assert {:retryable, %{type: "RuntimeError", message: "boom", details: nil}} =
             Default.classify(error, %{})
  end

  test "arbitrary error reasons classify as retryable" do
    assert {:retryable, %{type: "Error", message: msg, details: nil}} =
             Default.classify({:error, :timeout}, %{})

    assert msg == inspect({:error, :timeout})
  end

  test "impl/0 returns the configured Default module" do
    assert RetryClassifier.impl() == Hourglass.Activity.RetryClassifier.Default
  end

  test "classify/1 on the behaviour module delegates to the configured implementation" do
    assert {:retryable, %{type: "RuntimeError", message: "x", details: nil}} =
             RetryClassifier.classify(%RuntimeError{message: "x"})
  end
end
