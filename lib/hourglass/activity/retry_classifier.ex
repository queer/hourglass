defmodule Hourglass.Activity.RetryClassifier do
  @moduledoc """
  Behaviour for classifying an activity failure into a Temporal retry verdict.

  `ActivityRunner` resolves the implementation from
  `config :hourglass, :retry_classifier` (default
  `Hourglass.Activity.RetryClassifier.Default`). Context (originating activity
  name, caller site) is passed explicitly rather than via the process dictionary.
  """

  @type classification :: :retryable | :non_retryable | :unclassified
  @type metadata :: %{type: String.t(), message: String.t(), details: map() | nil}
  @type context :: %{
          optional(:activity_name) => String.t(),
          optional(:caller) => :rescue | :tuple_error
        }

  @callback classify(error :: term(), context :: context()) :: {classification(), metadata()}

  @doc "The configured classifier module."
  @spec impl() :: module()
  def impl, do: Application.get_env(:hourglass, :retry_classifier, __MODULE__.Default)

  @doc "Classify via the configured implementation."
  @spec classify(term(), context()) :: {classification(), metadata()}
  def classify(error, context \\ %{}), do: impl().classify(error, context)
end
