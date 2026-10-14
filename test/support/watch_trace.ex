defmodule Hourglass.TestSupport.WatchTrace do
  @moduledoc """
  What a closed-workflow watch asked Temporal, what Temporal answered, and
  when, so a test can tell whether the watch owed its bound.

  The watch ends an activity of a closed run within its bound when Temporal
  answers it in time: every question within a quarter of the bound, and with
  an answer rather than an error. It asks every quarter of the bound and stops
  an activity half the bound after learning its run closed, so a later answer
  leaves the bound behind; and a run Temporal does not answer for at all has
  not closed, as far as the watch knows (`Hourglass.Worker.ClosedWorkflowWatch`). The shared
  Temporal on a busy machine often does not answer in time; a test then
  asserts what does not depend on it, and that the activity was ended at all
  (`Hourglass.TestSupport.Patience`).

  It traces the watch: its calls that ask Temporal, and the answers it
  receives. The trace messages come to the process that started it.
  """

  alias Hourglass.Bridge

  @asks [
    {Bridge, :closed_runs_open_client, 4},
    {Bridge, :closed_runs_describe, 4}
  ]

  @doc "Start tracing `watch` to the calling process."
  @spec start(pid()) :: :ok
  def start(watch) do
    for mfa <- @asks, do: :erlang.trace_pattern(mfa, true, [:global])

    _traced =
      :erlang.trace(watch, true, [:call, :receive, :monotonic_timestamp, {:tracer, self()}])

    :ok
  end

  @doc """
  Whether Temporal has answered every question `watch` asked since the
  trace started within `answer_time` ms, with an answer: a run's state, or
  that it does not know the run.
  """
  @spec in_time?(pid(), pos_integer()) :: boolean()
  def in_time?(watch, answer_time) do
    {asked, answered} = drain(watch, %{}, [])
    now = System.monotonic_time(:millisecond)
    answered_tags = MapSet.new(answered, &elem(&1, 0))
    forgiven = Process.get({__MODULE__, :forgiven, watch}, MapSet.new())

    Enum.all?(answered, &answered_in_time?(&1, asked, forgiven, answer_time)) and
      not Enum.any?(asked, &unanswered_late?(&1, answered_tags, now, answer_time))
  end

  # An answer to a question asked before a reset is not judged. One asked
  # before the trace started took no one knows how long: it counts as late.
  defp answered_in_time?({tag, at, answer?}, asked, forgiven, answer_time) do
    case Map.fetch(asked, tag) do
      {:ok, asked_at} -> answer? and at - asked_at <= answer_time
      :error -> MapSet.member?(forgiven, tag)
    end
  end

  defp unanswered_late?({tag, asked_at}, answered_tags, now, answer_time),
    do: not MapSet.member?(answered_tags, tag) and now - asked_at > answer_time

  @doc """
  Forget what `watch` was asked and answered so far: the answers to questions
  asked until now are not judged.
  """
  @spec reset(pid()) :: :ok
  def reset(watch) do
    {asked, _answered} = drain(watch, %{}, [])
    key = {__MODULE__, :forgiven, watch}
    Process.put(key, MapSet.union(Process.get(key, MapSet.new()), MapSet.new(Map.keys(asked))))
    :ok
  end

  # The watch's questions (tag => when) and Temporal's answers ({tag, when,
  # an answer?}) traced so far, taken from the mailbox.
  defp drain(watch, asked, answered) do
    receive do
      {:trace_ts, ^watch, :call, {Bridge, _ask, [_client, _what, _timeout, tag]}, ts} ->
        drain(watch, Map.put(asked, tag, ms(ts)), answered)

      {:trace_ts, ^watch, :receive, message, ts} ->
        drain(watch, asked, answered(message, ms(ts), answered))
    after
      0 -> {asked, answered}
    end
  end

  defp answered({:hourglass_client_opened, tag, result}, at, answered),
    do: [{tag, at, match?({:ok, _client}, result)} | answered]

  defp answered({:hourglass_runs_described, tag, answers}, at, answered),
    do: [
      {tag, at, Enum.all?(answers, fn {_workflow, _run, answer} -> answer?(answer) end)}
      | answered
    ]

  defp answered(_other, _at, answered), do: answered

  defp answer?({:ok, _bytes}), do: true

  defp answer?({:error, %Bridge.Error{kind: :tonic_error, detail: detail}})
       when is_binary(detail),
       do: String.contains?(detail, "code: 'Some requested entity was not found'")

  defp answer?(_error), do: false

  defp ms(ts), do: System.convert_time_unit(ts, :nanosecond, :millisecond)
end
