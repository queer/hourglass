defmodule Hourglass.Activity.RetryClassifier.Default do
  @moduledoc """
  Permissive default classifier: every failure is `:retryable`. Hosts that need
  non-retryable verdicts for specific error shapes configure their own module
  via `config :hourglass, :retry_classifier`.
  """
  @behaviour Hourglass.Activity.RetryClassifier

  @impl Hourglass.Activity.RetryClassifier
  def classify(error, _context) when is_exception(error) do
    {:retryable,
     %{
       type:
         error.__struct__
         |> Module.split()
         |> List.last(),
       message: Exception.message(error),
       details: nil
     }}
  end

  def classify(reason, _context) do
    {:retryable, %{type: "Error", message: inspect(reason), details: nil}}
  end
end
