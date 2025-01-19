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

**To artifact reviewers:** please make sure you are run the following procedure on our provided platform.
All commands are assumed to be run at the project directory (i.e., where this README file resides).
In our provided AE platform, the project directory is `/home/gaoj/handlock`.

> `handlock` is ShiftLock's original name in development.

Please do not clone the repository to somewhere else.
This is because many experiment scripts have fixed absolute paths in them.

### Connect to our testbed

Our testbed is behind an OpenVPN gateway.
We have prepared an OpenVPN client profile for artifact reviewers on HotCRP.
Please first download [OpenVPN Connect](https://openvpn.net/client/) (or other OpenVPN clients if you prefer), import the profile, and connect to it.

> The OpenVPN profile will proxy `10.0.0.0/22`.

After you successfully connect to our lab's VPN, please SSH to our testbed:

```shell
ssh gaoj@10.0.2.110
```

Please also find the password on HotCRP.
**We highly recommend that you use Visual Studio Code to connect to the testbed.**

After you successfully SSH-ed to the testbed, please navigate to the project directory:

```shell
cd /home/gaoj/handlock
```

**IMPORTANT:** Please use `w` to check if there are any concurrent users of the testbed.

### Hello-world Example

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

If you can see this output, then everything is OK, and you can start the AE procedure.

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
