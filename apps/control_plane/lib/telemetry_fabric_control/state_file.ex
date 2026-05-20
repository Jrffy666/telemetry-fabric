defmodule TelemetryFabricControl.StateFile do
  @moduledoc """
  Small file-backed persistence helper for the MVP control plane.

  This is intentionally dependency-free. It gives the OTP stores a durable
  boundary now, while leaving room to replace the implementation with
  PostgreSQL/Ecto later without changing their public APIs.
  """

  @format_version 1

  def path(nil, _file), do: nil

  def path(storage_dir, file) when is_binary(storage_dir) and is_binary(file) do
    Path.join(storage_dir, file)
  end

  def load(nil, default), do: default

  def load(path, default) when is_binary(path) do
    case File.read(path) do
      {:ok, binary} ->
        decode!(path, binary)

      {:error, :enoent} ->
        default

      {:error, reason} ->
        raise File.Error, reason: reason, action: "read persisted state", path: path
    end
  end

  def persist(nil, _data), do: :ok

  def persist(path, data) when is_binary(path) do
    dir = Path.dirname(path)
    File.mkdir_p!(dir)

    tmp =
      Path.join(
        dir,
        ".#{Path.basename(path)}.tmp-#{System.unique_integer([:positive, :monotonic])}"
      )

    payload = :erlang.term_to_binary(%{version: @format_version, data: data})

    with :ok <- File.write(tmp, payload),
         :ok <- replace(tmp, path) do
      :ok
    else
      {:error, reason} ->
        _ = File.rm(tmp)
        raise File.Error, reason: reason, action: "persist state", path: path
    end
  end

  defp decode!(path, binary) do
    case :erlang.binary_to_term(binary, [:safe]) do
      %{version: @format_version, data: data} ->
        data

      other ->
        raise ArgumentError,
              "unsupported persisted state format in #{path}: #{inspect(other)}"
    end
  rescue
    error in ArgumentError ->
      reraise ArgumentError,
              [
                message:
                  "could not decode persisted state in #{path}: #{Exception.message(error)}"
              ],
              __STACKTRACE__
  end

  defp replace(tmp, path) do
    _ = File.rm(path)
    File.rename(tmp, path)
  end
end
