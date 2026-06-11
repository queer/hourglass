defmodule Hourglass.Proto.WorkerConfig do
  @moduledoc false

  use Protobuf,
    full_name: "hourglass.proto.WorkerConfig",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field :namespace, 1, type: :string
  field :task_queue, 2, type: :string, json_name: "taskQueue"
  field :max_cached_workflows, 3, type: :uint32, json_name: "maxCachedWorkflows"
  field :client_target_url, 4, type: :string, json_name: "clientTargetUrl"

  field :max_outstanding_workflow_tasks, 5,
    type: :uint32,
    json_name: "maxOutstandingWorkflowTasks"

  field :max_outstanding_activities, 6, type: :uint32, json_name: "maxOutstandingActivities"

  field :max_outstanding_local_activities, 7,
    type: :uint32,
    json_name: "maxOutstandingLocalActivities"
end
