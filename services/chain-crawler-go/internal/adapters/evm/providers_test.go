package evm

import "testing"

func TestProviderDefaultsCoverSupportedEVMChains(t *testing.T) {
	for _, chain := range []string{
		"ethereum",
		"bsc",
		"polygon",
		"arbitrum",
		"optimism",
		"base",
		"avalanche-c-chain",
	} {
		defaults, ok := ProviderDefaults(chain)
		if !ok {
			t.Fatalf("missing provider defaults for %s", chain)
		}
		if defaults.FinalityDepth == 0 || defaults.ReorgWindow == 0 || defaults.MaxBlockRange == 0 || defaults.InitialBlockRange == 0 {
			t.Fatalf("incomplete defaults for %s: %#v", chain, defaults)
		}
	}
}
