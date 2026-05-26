# Load Tests

Load tests should be explicit, opt-in, and separate from smoke tests. They can
use the mock RPC server for deterministic replay or a dedicated chain RPC
endpoint for environment-specific tests.

## Skeletons

- `load_test_plan.md`: target scenarios and acceptance criteria.
- `load_driver_skeleton.py`: stdlib driver that repeatedly queries a fixture
  RPC server and prints simple latency/count metrics.

## Example

```sh
python tests/fixtures/rpc/mock_evm_rpc.py \
  --scenario tests/fixtures/rpc/scenario_canonical.json \
  --port 18545

python tests/load/load_driver_skeleton.py \
  --rpc http://127.0.0.1:18545 \
  --from-block 0x10 \
  --to-block 0x13 \
  --iterations 100
```
