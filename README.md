# HarborMaster

This codebase implements the HarborMaster rollback detection protocol for confidential VMs.
The code is forked from PirateShip, a consensus protocol, but only for the performant data plane.
Hence, you will see certain consensus protocols implemented here (`src/consensus`), but those aren't part of HarborMaster.
The code may refer to the protocol as `PSL`, which is the older name for HarborMaster.

Clone this with `--recurse-submodules` to include our baseline, Nimble.

## Building

Requires `protoc`, `clang`, and Rust installed in your system.
Simply run `make` to build.


## Deployment

See `scripts` for instructions on how to deploy and run HarborMaster experiments. This requires an AWS subscription currently.
Although Azure can be used as well.

## Experiments

Run the experiments in `experiments/psl` to generate the graphs in the paper.
For Flink Experiments, move to the `flink-dev` branch and run `experiments/flink_experiment.toml`.
