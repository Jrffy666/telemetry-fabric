# Benchmarks

Benchmark harnesses compare CPU baselines against CUDA implementations using
representative batch sizes.

The phase 1 `benchmark_placeholder.cpp` executable only measures a CPU baseline
loop and prints the fields expected from future benchmark runs.

Required benchmark dimensions:

- input rows or graph edges per batch
- input bytes per batch
- host-to-device transfer time
- kernel execution time
- device-to-host transfer time
- end-to-end job latency
- rows or edges per second
- CPU fallback performance
- GPU memory high-water mark

Do not claim GPU speedups without a CPU baseline and realistic input sizes.
