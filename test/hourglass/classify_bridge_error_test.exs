defmodule Hourglass.ClassifyBridgeErrorTest do
  @moduledoc """
  Unit tests for `Hourglass.classify_bridge_error/1`.

  These tests do not require a live Temporal cluster — they only exercise the
  mapping from `Bridge.Error` structs to `Hourglass.Error` structs. Kept
  separate from `StatusTest` so they run in the default suite (no `:temporal`
  tag), unlike the cluster-dependent tests in `status_test.exs`.
  """

  use ExUnit.Case, async: true

  alias Hourglass.Bridge
  alias Hourglass.Error

  describe "classify_bridge_error/1" do
    test "tonic_error containing 'NotFound' maps to :not_found" do
      err = %Bridge.Error{
        kind: :tonic_error,
        detail: "Workflow execution NotFound for workflow_id 'foo'"
      }

      assert %Error{reason: :not_found, detail: "Workflow execution NotFound" <> _rest} =
               Hourglass.classify_bridge_error(err)
    end

    test "tonic_error containing lowercase 'not found' also maps to :not_found" do
      err = %Bridge.Error{kind: :tonic_error, detail: "namespace 'x' not found"}

      assert %Error{reason: :not_found, detail: "namespace 'x' not found"} =
               Hourglass.classify_bridge_error(err)
    end

    test "tonic_error with unrecognized detail maps to :rpc_error" do
      err = %Bridge.Error{kind: :tonic_error, detail: "DeadlineExceeded after 5s"}

      assert %Error{reason: :rpc_error, detail: "DeadlineExceeded after 5s"} =
               Hourglass.classify_bridge_error(err)
    end

    test "tonic_error with nil detail maps to :rpc_error with nil detail" do
      err = %Bridge.Error{kind: :tonic_error, detail: nil}

      assert %Error{reason: :rpc_error, detail: nil} =
               Hourglass.classify_bridge_error(err)
    end

    test "shutdown kind maps to :unreachable" do
      err = %Bridge.Error{kind: :shutdown, detail: nil}

      assert %Error{reason: :unreachable, detail: :shutdown} =
               Hourglass.classify_bridge_error(err)
    end

    test "unrecognized kind falls through to :rpc_error" do
      err = %Bridge.Error{kind: :invalid_proto, detail: "decode failed"}

      assert %Error{reason: :rpc_error, detail: ^err} =
               Hourglass.classify_bridge_error(err)
    end
  end
end
