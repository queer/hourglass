# credo:disable-for-this-file Credo.Check.Refactor.ModuleDependencies
# This module is a Credo check and requires Credo to be compiled first.
# Guard the definition so that when hourglass is used as a library dependency
# and Credo is not available (prod/non-dev contexts), this file compiles to
# a no-op stub rather than raising a compile error.
if Code.ensure_loaded?(Credo.Check) do
defmodule Hourglass.Check.WorkflowDeterminism do
  @moduledoc """
  Credo check: flags non-deterministic calls inside `use Hourglass.Workflow`
  modules. Workflow code replays — wallclock/RNG/IO/DB break replay. Move side
  effects into activities; use `Hourglass.Workflow.uuid/0` / `random/1` for RNG.
  Complements the compile-time hard lint (this is the configurable lint-time pass).
  """
  use Credo.Check, id: "HOURGLASS100", base_priority: :high, category: :warning

  @banned_module_calls [
    {DateTime, :utc_now},
    {DateTime, :now},
    {NaiveDateTime, :utc_now},
    {NaiveDateTime, :local_now},
    {Date, :utc_today},
    {Time, :utc_now},
    {System, :system_time},
    {System, :os_time},
    {System, :monotonic_time},
    {Process, :sleep},
    {Process, :send_after}
  ]

  @banned_atom_calls [
    {:rand, :uniform},
    {:rand, :normal},
    {:crypto, :strong_rand_bytes},
    {:erlang, :now},
    {:erlang, :system_time},
    {:erlang, :monotonic_time},
    {:erlang, :unique_integer},
    {:os, :timestamp},
    {:os, :system_time}
  ]

  @impl Credo.Check
  def run(%Credo.SourceFile{} = source_file, params \\ []) do
    issue_meta = IssueMeta.for(source_file, params)
    ast = Credo.Code.ast(source_file)
    issues_from_ast(ast, issue_meta)
  end

  # Walk top-level defmodule blocks, checking each for use Hourglass.Workflow
  # before linting. This scopes violations to only the modules that use the macro.
  defp issues_from_ast({:ok, ast}, issue_meta) do
    Credo.Code.prewalk(ast, &collect_module_issues(&1, &2, issue_meta), [])
  end

  defp issues_from_ast(_other, _issue_meta), do: []

  # When we encounter a defmodule, decide whether to lint it
  defp collect_module_issues({:defmodule, _meta, [_name, [do: body]]} = node, acc, issue_meta) do
    if module_body_uses_workflow?(body) do
      new_issues = Credo.Code.prewalk(body, &visit(&1, &2, issue_meta), [])
      {node, acc ++ new_issues}
    else
      {node, acc}
    end
  end

  defp collect_module_issues(node, acc, _issue_meta), do: {node, acc}

  # Check if the direct body of a module contains `use Hourglass.Workflow`.
  # We must NOT descend into nested `defmodule` blocks — only the top level of
  # this module's body counts, so a test file that nests a workflow module inside
  # an ExUnit test module is not itself treated as a workflow module.
  defp module_body_uses_workflow?(body) do
    Credo.Code.prewalk(
      body,
      fn
        {:use, _meta, [{:__aliases__, _alias_meta, [:Hourglass, :Workflow]} | _rest]} = node,
        _acc ->
          {node, true}

        # Stop descending into nested defmodule — its use directives are not ours
        {:defmodule, _meta, _args}, acc ->
          {nil, acc}

        node, acc ->
          {node, acc}
      end,
      false
    )
  end

  # Elixir-style call: DateTime.utc_now(), etc.
  defp visit(
         {{:., _dot_meta, [{:__aliases__, _alias_meta, parts}, fun]}, meta, _args} = ast,
         issues,
         issue_meta
       ) do
    # Analyses arbitrary user AST; the referenced module may not be a loaded
    # atom yet, so safe_concat/1 would raise. concat/1 is required here.
    # credo:disable-for-next-line Credo.Check.Warning.UnsafeToAtom
    mod = Module.concat(parts)

    if {mod, fun} in @banned_module_calls do
      {ast, [issue(mod, fun, meta, issue_meta) | issues]}
    else
      {ast, issues}
    end
  end

  # Erlang-style call: :rand.uniform(), :os.system_time(), etc.
  defp visit({{:., _dot_meta, [mod, fun]}, meta, _args} = ast, issues, issue_meta)
       when is_atom(mod) do
    if {mod, fun} in @banned_atom_calls do
      {ast, [issue(mod, fun, meta, issue_meta) | issues]}
    else
      {ast, issues}
    end
  end

  defp visit(ast, issues, _issue_meta), do: {ast, issues}

  defp issue(mod, fun, meta, issue_meta) do
    format_issue(issue_meta,
      message:
        "Non-deterministic #{inspect(mod)}.#{fun} in a workflow — " <>
          "move it into an activity, or use Hourglass.Workflow.uuid/0 / random/1.",
      line_no: meta[:line]
    )
  end
end
end
