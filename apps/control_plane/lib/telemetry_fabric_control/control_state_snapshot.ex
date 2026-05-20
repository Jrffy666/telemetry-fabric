defmodule TelemetryFabricControl.ControlStateSnapshot do
  @moduledoc """
  Immutable snapshot of the control-plane state.

  This is the handoff shape for future PostgreSQL/Ecto persistence. It lets the
  current OTP stores expose a consistent state image without leaking GenServer
  internals.
  """

  alias TelemetryFabricControl.AgentRegistry
  alias TelemetryFabricControl.AuditLog
  alias TelemetryFabricControl.CommandQueue
  alias TelemetryFabricControl.PipelineStore

  defstruct agents: [], pipeline_versions: [], pending_commands: [], audit_events: []

  def collect(opts \\ []) do
    agent_registry = Keyword.get(opts, :agent_registry, AgentRegistry)
    pipeline_store = Keyword.get(opts, :pipeline_store, PipelineStore)
    command_queue = Keyword.get(opts, :command_queue, CommandQueue)
    audit_log = Keyword.get(opts, :audit_log, AuditLog)

    %__MODULE__{
      agents: AgentRegistry.list_agents(agent_registry),
      pipeline_versions: PipelineStore.list_versions(pipeline_store),
      pending_commands: CommandQueue.list_all(command_queue),
      audit_events: AuditLog.list(audit_log, :all)
    }
  end
end
