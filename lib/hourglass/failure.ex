defmodule Hourglass.Failure do
  @moduledoc """
  Shared encoding for Temporal's `ApplicationFailureInfo`-flavoured
  `Temporal.Api.Failure.V1.Failure` proto.

  Temporal does not distinguish an activity's application failure from a
  workflow's: both travel as a `Failure` whose `failure_info` oneof carries an
  `ApplicationFailureInfo{type:, non_retryable:, details:}`. This module is
  the one place that shape gets built, so `Hourglass.ActivityRunner`
  (activity failures, via `Hourglass.Activity.RetryClassifier`'s verdict) and
  `Hourglass.Workflow.Evaluator` (a workflow body's own terminal failure, via
  `Hourglass.Workflow.fail/2,3`) encode it identically rather than
  maintaining two copies that could drift.
  """

  alias Temporal.Api.Common.V1.Payload
  alias Temporal.Api.Common.V1.Payloads
  alias Temporal.Api.Failure.V1.ApplicationFailureInfo
  alias Temporal.Api.Failure.V1.Failure

  @typedoc """
  The classifier/author-supplied shape this module encodes. `details`, when
  present, must be JSON-encodable (falls back to `inspect/1` otherwise).
  """
  @type application_failure :: %{
          type: String.t(),
          message: String.t(),
          details: map() | nil,
          non_retryable: boolean()
        }

  @doc """
  Build a `%Temporal.Api.Failure.V1.Failure{}` carrying an
  `ApplicationFailureInfo` from `application_failure()`.
  """
  @spec application_failure(application_failure()) :: Failure.t()
  def application_failure(%{
        type: type,
        message: message,
        details: details,
        non_retryable: non_retryable
      }) do
    application_info = %ApplicationFailureInfo{
      type: type,
      non_retryable: non_retryable,
      details: encode_details(details)
    }

    %Failure{
      message: message,
      source: type,
      failure_info: {:application_failure_info, application_info}
    }
  end

  # `details` is `ApplicationFailureInfo.details :: Payloads`. A non-nil
  # details map is encoded as a single json/plain Payload so downstream
  # consumers (history inspectors, replay tooling) can decode it. Nil → no
  # Payloads at all. Unencodable maps fall back to inspect to preserve at
  # least a textual trail.
  @spec encode_details(map() | nil) :: Payloads.t() | nil
  defp encode_details(nil), do: nil

  defp encode_details(details) when is_map(details) do
    data =
      case Jason.encode(details) do
        {:ok, json} -> json
        {:error, _reason} -> Jason.encode!(%{inspect: inspect(details)})
      end

    payload = %Payload{metadata: %{"encoding" => "json/plain"}, data: data}
    %Payloads{payloads: [payload]}
  end
end
