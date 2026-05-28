defmodule TelemetryFabricControl.PostgresSync do
  @moduledoc """
  Periodically syncs the OTP control-plane state into PostgreSQL.

  This process writes consistent snapshots through Ecto when PostgreSQL is
  configured alongside dependency-free OTP stores.
  """

  use GenServer

  require Logger

  alias TelemetryFabricControl.ControlStateSnapshot
  alias TelemetryFabricControl.PostgresWriter
  alias TelemetryFabricControl.Repo

  @default_interval_ms 5_000

  def start_link(opts) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  def sync_once(opts \\ []) do
    repo = Keyword.get(opts, :repo, Repo)
    snapshot = Keyword.get_lazy(opts, :snapshot, fn -> collect_snapshot(opts) end)

    snapshot
    |> PostgresWriter.to_multi()
    |> repo.transaction()
  end

  @impl true
  def init(opts) do
    state = %{
      interval_ms: Keyword.get(opts, :interval_ms, @default_interval_ms),
      repo: Keyword.get(opts, :repo, Repo),
      snapshot_opts: Keyword.get(opts, :snapshot_opts, [])
    }

    if Keyword.get(opts, :sync_on_start, true) do
      send(self(), :sync)
    end

    {:ok, state}
  end

  @impl true
  def handle_info(:sync, state) do
    state
    |> sync_opts()
    |> sync_once()
    |> log_sync_result()

    schedule_next_sync(state)
    {:noreply, state}
  end

  defp collect_snapshot(opts) do
    opts
    |> Keyword.get(:snapshot_opts, [])
    |> ControlStateSnapshot.collect()
  end

  defp sync_opts(state) do
    [repo: state.repo, snapshot_opts: state.snapshot_opts]
  end

  defp log_sync_result({:ok, _changes}), do: :ok

  defp log_sync_result({:error, operation, reason, _changes}) do
    Logger.warning(
      "PostgreSQL control-plane sync failed at #{inspect(operation)}: #{inspect(reason)}"
    )
  end

  defp log_sync_result({:error, reason}) do
    Logger.warning("PostgreSQL control-plane sync failed: #{inspect(reason)}")
  end

  defp schedule_next_sync(%{interval_ms: interval_ms})
       when is_integer(interval_ms) and interval_ms > 0 do
    Process.send_after(self(), :sync, interval_ms)
  end

  defp schedule_next_sync(_state), do: :ok
end
