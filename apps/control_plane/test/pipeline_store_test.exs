defmodule TelemetryFabricControl.PipelineStoreTest do
  use ExUnit.Case

  alias TelemetryFabricControl.PipelineStore
  alias TelemetryFabricControl.SamplePipeline

  setup do
    TelemetryFabricControl.AuditLog.clear()
    TelemetryFabricControl.CommandQueue.clear()
    PipelineStore.clear()
    :ok
  end

  test "stores versioned pipeline configs" do
    config = SamplePipeline.build("payments-prod")

    assert {:ok, first} = PipelineStore.put_pipeline(config, "test")
    assert first.version == 1

    assert {:ok, second} = PipelineStore.put_pipeline(config, "test")
    assert second.version == 2

    assert {:ok, latest} = PipelineStore.get_pipeline("payments-prod", "default")
    assert latest.version == 2
  end

  test "persists pipeline versions across process restarts" do
    dir = tmp_dir("pipeline-store")
    path = Path.join(dir, "pipelines.term")
    name = unique_name("pipeline_store")
    restarted_name = unique_name("pipeline_store")
    config = SamplePipeline.build("payments-prod")

    {:ok, pid} = PipelineStore.start_link(name: name, storage_path: path)

    assert {:ok, first} = PipelineStore.put_pipeline(name, config, "test")
    assert first.version == 1

    GenServer.stop(pid)
    {:ok, _pid} = PipelineStore.start_link(name: restarted_name, storage_path: path)

    assert {:ok, latest} = PipelineStore.get_pipeline(restarted_name, "payments-prod", "default")
    assert latest.version == 1
  end

  test "persists audit events across process restarts" do
    dir = tmp_dir("audit-log")
    path = Path.join(dir, "audit.term")
    name = unique_name("audit_log")
    restarted_name = unique_name("audit_log")

    {:ok, pid} = TelemetryFabricControl.AuditLog.start_link(name: name, storage_path: path)

    assert {:ok, event} =
             TelemetryFabricControl.AuditLog.append(name, %{
               actor: "test",
               action: "pipeline.updated",
               resource: "payments-prod/default",
               metadata: %{version: 1}
             })

    assert event.action == "pipeline.updated"

    GenServer.stop(pid)

    {:ok, _pid} =
      TelemetryFabricControl.AuditLog.start_link(name: restarted_name, storage_path: path)

    assert [%TelemetryFabricControl.AuditLog{action: "pipeline.updated"}] =
             TelemetryFabricControl.AuditLog.list(restarted_name, 10)
  end

  defp unique_name(prefix) do
    String.to_atom("#{prefix}_#{System.unique_integer([:positive, :monotonic])}")
  end

  defp tmp_dir(name) do
    root = Path.expand("../../../.tmp/control_plane_tests", __DIR__)
    path = Path.join(root, "#{name}-#{System.unique_integer([:positive, :monotonic])}")
    File.rm_rf!(path)
    File.mkdir_p!(path)
    on_exit(fn -> File.rm_rf!(path) end)
    path
  end
end
