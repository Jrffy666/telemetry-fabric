defmodule TelemetryFabricControl.MixWindows do
  @moduledoc false

  def run(args) do
    repo_root = Path.expand("../../..", __DIR__)
    app_dir = Path.join(repo_root, "apps/control_plane")
    patch_file_mkdir_p_windows()
    System.put_env("MIX_ENV", System.get_env("MIX_ENV") || "test")
    Mix.start()
    patch_mix_absolute_tmp_bug()

    task = List.first(args) || "test"
    task_args = Enum.drop(args, 1)

    File.cd!(repo_root)

    Mix.Project.in_project(:telemetry_fabric_control, app_dir, fn _project ->
      Mix.Task.run(task, task_args)
    end)
  end

  defp patch_file_mkdir_p_windows do
    source_path = elixir_source_path(File, "file.ex")
    source = File.read!(source_path)

    source =
      String.replace(
        source,
        """
          def mkdir_p(path) do
            do_mkdir_p(IO.chardata_to_string(path))
          end
        """,
        """
          def mkdir_p(path) do
            do_mkdir_p(normalize_mkdir_p_path(IO.chardata_to_string(path)))
          end

          defp normalize_mkdir_p_path(path) do
            path
            |> Path.expand()
            |> relative_to_cwd()
          end

          defp relative_to_cwd(path) do
            {:ok, cwd} = :file.get_cwd()
            cwd = Path.expand(List.to_string(cwd))

            case descendant_parts(path_parts(path), path_parts(cwd)) do
              :not_relative -> path
              [] -> "."
              parts -> Path.join(parts)
            end
          end

          defp descendant_parts(path_tail, []), do: path_tail

          defp descendant_parts([path_head | path_tail], [cwd_head | cwd_tail]) do
            if String.downcase(path_head) == String.downcase(cwd_head) do
              descendant_parts(path_tail, cwd_tail)
            else
              :not_relative
            end
          end

          defp descendant_parts(_path_parts, _cwd_parts), do: :not_relative

          defp path_parts(path) do
            path
            |> String.replace("\\\\", "/")
            |> String.split("/", trim: true)
          end
        """
      )

    needle = """
      defp do_mkdir_p("/") do
        :ok
      end
    """

    replacement = """
      defp do_mkdir_p("/") do
        :ok
      end

      defp do_mkdir_p(<<drive, ":/">>) when drive in ?a..?z or drive in ?A..?Z do
        :ok
      end

      defp do_mkdir_p(<<drive, ":">>) when drive in ?a..?z or drive in ?A..?Z do
        :ok
      end
    """

    unless String.contains?(source, needle) do
      raise "could not patch File.mkdir_p/1; target expression was not found"
    end

    source
    |> String.replace(needle, replacement)
    |> String.replace(
      """
                  {:error, :eexist} ->
                    if dir?(path), do: :ok, else: {:error, :enotdir}
      """,
      """
                  {:error, :eexist} ->
                    :ok
      """
    )
    |> Code.compile_string(source_path)
  end

  defp patch_mix_absolute_tmp_bug do
    # Elixir 1.19.5/OTP 28 on this Windows install returns {:error, :enotdir}
    # from File.mkdir_p/1 for absolute paths that already exist. Mix.Sync uses
    # System.tmp_dir!/0, so patch its lock/pubsub temp roots to relative paths.
    patch_source(
      Mix.Sync.PubSub,
      "mix/sync/pubsub.ex",
      ~S|Path.join(System.tmp_dir!(), "mix_pubsub_user#{Mix.Utils.detect_user_id!()}")|,
      ~S|Path.join([".mix_tmp", "mix_pubsub_user"])|
    )

    patch_source(
      Mix.Sync.Lock,
      "mix/sync/lock.ex",
      ~S|Path.join(System.tmp_dir!(), "mix_lock_user#{Mix.Utils.detect_user_id!()}")|,
      ~S|Path.join([".mix_tmp", "mix_lock_user"])|
    )
  end

  defp patch_source(module, relative_source, needle, replacement) do
    source_path = source_path(module, relative_source)
    source = File.read!(source_path)

    unless String.contains?(source, needle) do
      raise "could not patch #{inspect(module)}; target expression was not found"
    end

    source
    |> String.replace(needle, replacement)
    |> Code.compile_string(source_path)
  end

  defp source_path(module, relative_source) do
    module
    |> :code.which()
    |> List.to_string()
    |> Path.dirname()
    |> then(&Path.join([&1, "..", "lib", relative_source]))
    |> Path.expand()
  end

  defp elixir_source_path(module, relative_source) do
    module
    |> :code.which()
    |> List.to_string()
    |> Path.dirname()
    |> then(&Path.join([&1, "..", "lib", relative_source]))
    |> Path.expand()
  end
end

TelemetryFabricControl.MixWindows.run(System.argv())
