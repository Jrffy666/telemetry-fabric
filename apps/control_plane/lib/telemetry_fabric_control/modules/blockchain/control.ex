defmodule TelemetryFabricControl.Modules.Blockchain.Control do
  @moduledoc """
  Blockchain module control API.

  The blockchain namespace owns chain, endpoint, watchlist, filter, assignment,
  and checkpoint concepts. The surrounding control-plane core remains generic.
  """

  alias TelemetryFabricControl.Modules.Blockchain.Store

  def list_chains(tenant_id, opts \\ []), do: Store.list(:chains, tenant_id, opts)
  def get_chain(tenant_id, chain_key), do: Store.get(:chains, tenant_id, chain_key)
  def upsert_chain(attrs, actor \\ "operator"), do: Store.upsert(:chains, attrs, actor)

  def delete_chain(tenant_id, chain_key, actor \\ "operator"),
    do: Store.delete(:chains, tenant_id, chain_key, actor)

  def list_rpc_endpoints(tenant_id, opts \\ []), do: Store.list(:rpc_endpoints, tenant_id, opts)

  def get_rpc_endpoint(tenant_id, endpoint_id),
    do: Store.get(:rpc_endpoints, tenant_id, endpoint_id)

  def upsert_rpc_endpoint(attrs, actor \\ "operator"),
    do: Store.upsert(:rpc_endpoints, attrs, actor)

  def delete_rpc_endpoint(tenant_id, endpoint_id, actor \\ "operator"),
    do: Store.delete(:rpc_endpoints, tenant_id, endpoint_id, actor)

  def list_address_watchlist(tenant_id, opts \\ []),
    do: Store.list(:address_watchlist, tenant_id, opts)

  def get_address_watch(tenant_id, entry_id),
    do: Store.get(:address_watchlist, tenant_id, entry_id)

  def upsert_address_watch(attrs, actor \\ "operator"),
    do: Store.upsert(:address_watchlist, attrs, actor)

  def delete_address_watch(tenant_id, entry_id, actor \\ "operator"),
    do: Store.delete(:address_watchlist, tenant_id, entry_id, actor)

  def list_contract_watchlist(tenant_id, opts \\ []),
    do: Store.list(:contract_watchlist, tenant_id, opts)

  def get_contract_watch(tenant_id, contract_id),
    do: Store.get(:contract_watchlist, tenant_id, contract_id)

  def upsert_contract_watch(attrs, actor \\ "operator"),
    do: Store.upsert(:contract_watchlist, attrs, actor)

  def delete_contract_watch(tenant_id, contract_id, actor \\ "operator"),
    do: Store.delete(:contract_watchlist, tenant_id, contract_id, actor)

  def list_token_watchlist(tenant_id, opts \\ []),
    do: Store.list(:token_watchlist, tenant_id, opts)

  def get_token_watch(tenant_id, token_id), do: Store.get(:token_watchlist, tenant_id, token_id)

  def upsert_token_watch(attrs, actor \\ "operator"),
    do: Store.upsert(:token_watchlist, attrs, actor)

  def delete_token_watch(tenant_id, token_id, actor \\ "operator"),
    do: Store.delete(:token_watchlist, tenant_id, token_id, actor)

  def list_filter_rules(tenant_id, opts \\ []), do: Store.list(:filter_rules, tenant_id, opts)
  def get_filter_rule(tenant_id, rule_id), do: Store.get(:filter_rules, tenant_id, rule_id)

  def upsert_filter_rule(attrs, actor \\ "operator"),
    do: Store.upsert(:filter_rules, attrs, actor)

  def delete_filter_rule(tenant_id, rule_id, actor \\ "operator"),
    do: Store.delete(:filter_rules, tenant_id, rule_id, actor)

  def list_crawl_assignments(tenant_id, opts \\ []),
    do: Store.list(:crawl_assignments, tenant_id, opts)

  def get_crawl_assignment(tenant_id, assignment_id),
    do: Store.get(:crawl_assignments, tenant_id, assignment_id)

  def upsert_crawl_assignment(attrs, actor \\ "operator"),
    do: Store.upsert(:crawl_assignments, attrs, actor)

  def delete_crawl_assignment(tenant_id, assignment_id, actor \\ "operator"),
    do: Store.delete(:crawl_assignments, tenant_id, assignment_id, actor)

  def list_checkpoints(tenant_id, opts \\ []), do: Store.list(:checkpoints, tenant_id, opts)

  def get_checkpoint(tenant_id, assignment_id),
    do: Store.get(:checkpoints, tenant_id, assignment_id)

  def put_checkpoint(attrs, actor \\ "crawler"), do: Store.upsert(:checkpoints, attrs, actor)
end
