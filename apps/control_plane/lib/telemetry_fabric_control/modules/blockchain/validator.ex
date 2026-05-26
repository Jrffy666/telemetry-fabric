defmodule TelemetryFabricControl.Modules.Blockchain.Validator do
  @moduledoc """
  Blockchain module validation and redaction helpers.

  Keeping these checks in the blockchain namespace prevents chain-specific
  fields from spreading into platform core.
  """

  @secret_query_keys ~w(api_key apikey key token access_token secret password)

  def validate_config(config) when is_map(config) do
    errors =
      []
      |> validate_optional_list(config, "chains")
      |> validate_optional_list(config, "rpc_endpoints")
      |> validate_optional_list(config, "address_watchlist")
      |> validate_optional_list(config, "contract_watchlist")
      |> validate_optional_list(config, "token_watchlist")
      |> validate_optional_list(config, "filter_rules")
      |> validate_optional_list(config, "crawl_assignments")

    if errors == [], do: :ok, else: {:error, {:validation_failed, Enum.reverse(errors)}}
  end

  def validate_record(:chains, record) do
    []
    |> require_text(record, "tenant_id")
    |> require_text(record, "chain_key")
    |> require_text(record, "display_name")
    |> require_text(record, "network")
    |> validate_identifier(record, "chain_key")
    |> result()
  end

  def validate_record(:rpc_endpoints, record) do
    []
    |> require_text(record, "tenant_id")
    |> require_text(record, "endpoint_id")
    |> require_text(record, "chain_key")
    |> require_text(record, "url")
    |> validate_identifier(record, "endpoint_id")
    |> validate_identifier(record, "chain_key")
    |> validate_url(record, "url")
    |> result()
  end

  def validate_record(:address_watchlist, record) do
    validate_address_record(record, "entry_id", "address")
  end

  def validate_record(:contract_watchlist, record) do
    validate_address_record(record, "contract_id", "address")
  end

  def validate_record(:token_watchlist, record) do
    []
    |> require_text(record, "tenant_id")
    |> require_text(record, "token_id")
    |> require_text(record, "chain_key")
    |> require_text(record, "contract_address")
    |> validate_identifier(record, "token_id")
    |> validate_identifier(record, "chain_key")
    |> validate_address(record, "contract_address")
    |> result()
  end

  def validate_record(:filter_rules, record) do
    []
    |> require_text(record, "tenant_id")
    |> require_text(record, "rule_id")
    |> require_text(record, "name")
    |> validate_identifier(record, "rule_id")
    |> validate_filter_action(record)
    |> validate_map(record, "expression")
    |> result()
  end

  def validate_record(:crawl_assignments, record) do
    []
    |> require_text(record, "tenant_id")
    |> require_text(record, "assignment_id")
    |> require_text(record, "chain_key")
    |> require_text(record, "crawler_id")
    |> validate_identifier(record, "assignment_id")
    |> validate_identifier(record, "chain_key")
    |> result()
  end

  def validate_record(:checkpoints, record) do
    []
    |> require_text(record, "tenant_id")
    |> require_text(record, "assignment_id")
    |> require_text(record, "chain_key")
    |> validate_map(record, "cursor")
    |> result()
  end

  def redact_record(:rpc_endpoints, record), do: Map.update(record, "url", nil, &redact_rpc_url/1)
  def redact_record(_kind, record), do: record

  def redact_rpc_url(url) when is_binary(url) do
    case URI.parse(url) do
      %URI{scheme: scheme, host: host} = uri when is_binary(scheme) and is_binary(host) ->
        redacted_query =
          uri.query
          |> decode_query()
          |> Enum.map(fn {key, value} ->
            if String.downcase(key) in @secret_query_keys do
              {key, "[REDACTED]"}
            else
              {key, value}
            end
          end)
          |> URI.encode_query()

        %{uri | userinfo: redact_userinfo(uri.userinfo), query: blank_to_nil(redacted_query)}
        |> URI.to_string()

      _ ->
        url
    end
  end

  def redact_rpc_url(value), do: value

  defp validate_address_record(record, id_field, address_field) do
    []
    |> require_text(record, "tenant_id")
    |> require_text(record, id_field)
    |> require_text(record, "chain_key")
    |> require_text(record, address_field)
    |> validate_identifier(record, id_field)
    |> validate_identifier(record, "chain_key")
    |> validate_address(record, address_field)
    |> result()
  end

  defp validate_optional_list(errors, config, key) do
    case Map.get(config, key, Map.get(config, String.to_atom(key))) do
      nil -> errors
      value when is_list(value) -> errors
      _value -> ["#{key}_must_be_list" | errors]
    end
  end

  defp require_text(errors, record, field) do
    case Map.get(record, field) do
      value when is_binary(value) ->
        if String.trim(value) == "", do: ["#{field}_required" | errors], else: errors

      _value ->
        ["#{field}_required" | errors]
    end
  end

  defp validate_identifier(errors, record, field) do
    value = Map.get(record, field, "")

    if is_binary(value) and Regex.match?(~r/^[a-zA-Z0-9][a-zA-Z0-9_.:-]*$/, value) do
      errors
    else
      ["#{field}_invalid" | errors]
    end
  end

  defp validate_address(errors, record, field) do
    value = Map.get(record, field, "")

    if is_binary(value) and Regex.match?(~r/^0x[0-9a-fA-F]{40}$/, value) do
      errors
    else
      ["#{field}_invalid" | errors]
    end
  end

  defp validate_url(errors, record, field) do
    value = Map.get(record, field, "")

    case URI.parse(value) do
      %URI{scheme: scheme, host: host}
      when scheme in ["http", "https", "ws", "wss"] and is_binary(host) ->
        errors

      _ ->
        ["#{field}_invalid" | errors]
    end
  end

  defp validate_map(errors, record, field) do
    if is_map(Map.get(record, field)), do: errors, else: ["#{field}_must_be_map" | errors]
  end

  defp validate_filter_action(errors, record) do
    case Map.get(record, "action", "keep") do
      action when action in ["keep", "drop"] -> errors
      _action -> ["action_invalid" | errors]
    end
  end

  defp result([]), do: :ok
  defp result(errors), do: {:error, {:validation_failed, Enum.reverse(errors)}}

  defp decode_query(nil), do: []
  defp decode_query(""), do: []
  defp decode_query(query), do: URI.decode_query(query)

  defp redact_userinfo(nil), do: nil
  defp redact_userinfo(""), do: nil
  defp redact_userinfo(_userinfo), do: "[REDACTED]"

  defp blank_to_nil(""), do: nil
  defp blank_to_nil(value), do: value
end
