defmodule TelemetryFabricControl.Json do
  @moduledoc """
  Small dependency-free JSON codec for the MVP HTTP control adapter.

  This deliberately supports only standard JSON values and returns object keys as
  strings. Transport adapters normalize keys before calling the domain API.
  """

  def decode!(binary) when is_binary(binary) do
    {value, rest} = parse_value(skip_ws(binary))

    case skip_ws(rest) do
      "" -> value
      _ -> raise ArgumentError, "unexpected trailing JSON input"
    end
  end

  def encode!(value) do
    IO.iodata_to_binary(encode_value(value))
  end

  defp parse_value(<<"null", rest::binary>>), do: {nil, rest}
  defp parse_value(<<"true", rest::binary>>), do: {true, rest}
  defp parse_value(<<"false", rest::binary>>), do: {false, rest}
  defp parse_value(<<"\"", rest::binary>>), do: parse_string(rest, [])
  defp parse_value(<<"{", rest::binary>>), do: parse_object(skip_ws(rest), %{})
  defp parse_value(<<"[", rest::binary>>), do: parse_array(skip_ws(rest), [])

  defp parse_value(<<char, _rest::binary>> = input) when char in ~c"-0123456789" do
    parse_number(input)
  end

  defp parse_value(_input), do: raise(ArgumentError, "invalid JSON value")

  defp parse_object(<<"}", rest::binary>>, acc), do: {acc, rest}

  defp parse_object(<<"\"", rest::binary>>, acc) do
    {key, rest} = parse_string(rest, [])
    rest = skip_ws(rest)

    case rest do
      <<":", rest::binary>> ->
        {value, rest} = parse_value(skip_ws(rest))
        rest = skip_ws(rest)
        acc = Map.put(acc, key, value)

        case rest do
          <<",", rest::binary>> -> parse_object(skip_ws(rest), acc)
          <<"}", rest::binary>> -> {acc, rest}
          _ -> raise ArgumentError, "expected comma or object terminator"
        end

      _ ->
        raise ArgumentError, "expected object key separator"
    end
  end

  defp parse_object(_input, _acc), do: raise(ArgumentError, "expected object key")

  defp parse_array(<<"]", rest::binary>>, acc), do: {Enum.reverse(acc), rest}

  defp parse_array(input, acc) do
    {value, rest} = parse_value(input)
    rest = skip_ws(rest)

    case rest do
      <<",", rest::binary>> -> parse_array(skip_ws(rest), [value | acc])
      <<"]", rest::binary>> -> {Enum.reverse([value | acc]), rest}
      _ -> raise ArgumentError, "expected comma or array terminator"
    end
  end

  defp parse_string(<<"\"", rest::binary>>, acc) do
    {acc |> Enum.reverse() |> IO.iodata_to_binary(), rest}
  end

  defp parse_string(<<"\\\"", rest::binary>>, acc), do: parse_string(rest, [?\" | acc])
  defp parse_string(<<"\\\\", rest::binary>>, acc), do: parse_string(rest, [?\\ | acc])
  defp parse_string(<<"\\/", rest::binary>>, acc), do: parse_string(rest, [?/ | acc])
  defp parse_string(<<"\\b", rest::binary>>, acc), do: parse_string(rest, [?\b | acc])
  defp parse_string(<<"\\f", rest::binary>>, acc), do: parse_string(rest, [?\f | acc])
  defp parse_string(<<"\\n", rest::binary>>, acc), do: parse_string(rest, [?\n | acc])
  defp parse_string(<<"\\r", rest::binary>>, acc), do: parse_string(rest, [?\r | acc])
  defp parse_string(<<"\\t", rest::binary>>, acc), do: parse_string(rest, [?\t | acc])

  defp parse_string(<<"\\u", hex::binary-size(4), rest::binary>>, acc) do
    {codepoint, ""} = Integer.parse(hex, 16)
    parse_string(rest, [<<codepoint::utf8>> | acc])
  end

  defp parse_string(<<char::utf8, rest::binary>>, acc),
    do: parse_string(rest, [<<char::utf8>> | acc])

  defp parse_string(<<>>, _acc), do: raise(ArgumentError, "unterminated JSON string")

  defp parse_number(input) do
    {token, rest} = take_number(input, [])

    value =
      if String.contains?(token, [".", "e", "E"]) do
        String.to_float(token)
      else
        String.to_integer(token)
      end

    {value, rest}
  end

  defp take_number(<<char, rest::binary>>, acc)
       when char in ~c"-+0123456789.eE",
       do: take_number(rest, [char | acc])

  defp take_number(rest, acc), do: {acc |> Enum.reverse() |> IO.iodata_to_binary(), rest}

  defp skip_ws(<<char, rest::binary>>) when char in ~c" \n\r\t", do: skip_ws(rest)
  defp skip_ws(input), do: input

  defp encode_value(nil), do: "null"
  defp encode_value(true), do: "true"
  defp encode_value(false), do: "false"
  defp encode_value(value) when is_integer(value) or is_float(value), do: to_string(value)
  defp encode_value(value) when is_binary(value), do: [?\", escape(value), ?\"]

  defp encode_value(value) when is_atom(value) do
    value |> Atom.to_string() |> encode_value()
  end

  defp encode_value(value) when is_list(value) do
    ["[", value |> Enum.map(&encode_value/1) |> Enum.intersperse(","), "]"]
  end

  defp encode_value(value) when is_map(value) do
    entries =
      value
      |> Enum.sort_by(fn {key, _value} -> to_string(key) end)
      |> Enum.map(fn {key, item} ->
        [encode_value(to_string(key)), ":", encode_value(item)]
      end)
      |> Enum.intersperse(",")

    ["{", entries, "}"]
  end

  defp escape(value) do
    value
    |> String.replace("\\", "\\\\")
    |> String.replace("\"", "\\\"")
    |> String.replace("\n", "\\n")
    |> String.replace("\r", "\\r")
    |> String.replace("\t", "\\t")
  end
end
