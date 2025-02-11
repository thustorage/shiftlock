# [FAST'25 Artifact] ShiftLock: Mitigate One-sided RDMA Lock Contention via Handover

Welcome to the artifact repository of FAST'25 accepted paper: _ShiftLock: Mitigate One-sided RDMA Lock Contention via Handover_!

Should there be any questions, please contact the authors in HotCRP.
The authors will respond to each question within 24hrs and as soon as possible.

## Environment Setup

**To artifact reviewers:** please skip this section and go to "[Evaluate the Artifact](#evaluate-the-Artifact)".
This is because have already set up the required environment on the provided platform.

### Prerequisites

- A cluster of at least 6 machines connected with Ethernet (Internet must be available)
- Infiniband RDMA network
- Mellanox ConnectX-5 or newer RDMA NICs
- MLNX OFED v4.x (with extended atomics support), which can be downloaded [here](https://network.nvidia.com/products/infiniband-drivers/linux/mlnx_ofed/) in the "Archived Versions" tab
- Rust 1.83

## Evaluate the Artifact

### Hello-world example

To verify that everything is prepared, you can run a hello-world example that verifies ShiftLock's functionality, please run the following command:

```shell
scripts/ae/hello_world.sh
```

It will run for approximately 10 seconds and, on success, output something like below:

```
NOTE: this is a hello-world example for ShiftLock.
      for real distributed benchmarking, run `server` and `client`.
Running for 10 seconds...
OK: 8 threads: 738528 locks/s
```

If you can see this output, then everything is OK, and you can start running the artifact.

### Cluster setup

After you confirm that the hello-world example works, it is time to set up the cluster.
See the following checklist:

1. Find some nodes that are connected by Infiniband network and Ethernet. _RoCE currently does not work._
2. Ensure they have the same version of Rust and glibc.
3. Pick a server node. All scripts must be run on it.
4. Clone the repository to the same paths on all nodes and compile them (`cargo build --release`). You may use NFS, but pay attention to write permissions.
5. Ensure password-free SSH from server to all clients.
6. Go to `scripts/utils/set-nodes.sh`. Modify the comma-separated IP list to those of your _clients_ (EXCLUDING SERVER!). Ethernet interface IP is OK.
7. Ensure your RDMA NICs are all `mlx5_0`.
   - If they are not `mlx5_0` but still the same across the entire cluster, please search and replace this string in the entire repository.
   - Otherwise, it will be much more troublesome. Briefly speaking, you need to pass an `--dev` option to all server and client executables and modify `run-counters.sh`.

### Run all experiments

There is an all-in-one AE script for your convenience:

```shell
scripts/ae/run_all.sh
```

This script will run for approximately 1.5 hours and store all results in the `data` directory.

> We have prepared 8 scripts that run different experiments to reproduce all figures in our paper, which are the `scripts/ae/expX.sh` files (X = 1, 2, ..., 8).
> The all-in-one script simply invocates them one by one.
> If you want to run individual experiments, please refer to these script files and the comments in them (which describes the relationship between experiments and figures/tables).

### Plot the figures & tables

#### (Recommended) For Visual Studio Code users

Please install the Jupyter extension in VSCode.
Then, please open `scripts/ae/plotting.ipynb`.

Please use the Python 3.6.9 (`/usr/bin/python3`) kernel.

Then, you can run each cell from top to bottom.
The first cell contains prelude functions and definitions, so please run it first.
Each other cell plots a figure or table.

#### For others

Please run the plotter script in the `scripts/ae` directory:

```shell
cd scripts/ae
python3 plotting.py
```

The command above will plot all figures and tables by default, and the results will be stored in the `figs` directory.
So, please ensure that you have finished running the all-in-one AE script before running the plotter.

The plotter supports specifying certain figures or tables to plot by command-line arguments.
For example:

```
python3 plotting.py figure3 table4 table3
```

Please refer to Lines 574~614 of `plotting.py` for accepted arguments.

### Cleanup

After finishing your AE procedure, please clean up your results:

```shell
scripts/ae/clear.sh
```

## Cite our paper

Jian Gao, Qing Wang, and Jiwu Shu. _ShiftLock: Mitigate One-sided RDMA Lock Contention via Handover_. To appear in the 23rd USENIX
Conference on File and Storage Technologies (FAST ’25), Santa Clara CA USA, February 2025. USENIX. https://www.usenix.org/conference/fast25/presentation/gao.
