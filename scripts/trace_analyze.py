#!/usr/bin/python3

import glob
import sys
from functools import reduce

trace_path = '$SCRIPT_DIR/../traces'


def main(workload: str):
    max_cons = 0
    max_lock_id = 0

    txn_cnt = [0] * 7
    lock_cnt = [0] * 7
    lock_occ = {}
    lock_ty = {1: 0, 2: 0}
    written = set()

    for f in glob.glob(trace_path + f'/*{workload}.csv'):
        print('Processing', f)
        last_id = ""
        last_ty = -1
        cons = 0
        for line in open(f).readlines():
            tokens = line.split(',')
            id = tokens[0]
            ty = int(tokens[2])
            lock_id = int(tokens[3])
            lock_se = int(tokens[4])

            if lock_id not in lock_occ:
                lock_occ[lock_id] = 0
            lock_occ[lock_id] += 1

            lock_ty[lock_se] = lock_ty[lock_se] + 1
            if lock_se == 1:
                written.add(lock_id)

            if id == last_id:
                cons += 1
            else:
                if cons > max_cons:
                    max_cons = cons
                if last_ty != -1:
                    txn_cnt[last_ty - 1] += 1
                    lock_cnt[last_ty - 1] += cons
                cons = 1
            
            last_id = id
            last_ty = ty

            lock_id = int(tokens[3])
            if lock_id > max_lock_id:
                max_lock_id = lock_id
            
    print()
    print(f'max_cons = {max_cons}, #locks = {max_lock_id + 1}')
    for i in range(7):
        if lock_cnt[i] == 0:
            break
        print(f'type {i + 1}: txns = {txn_cnt[i]}, #locks/txn = {lock_cnt[i] / txn_cnt[i]}')

    occ = sorted(list(lock_occ.values()))
    print(f'max-occ = {occ[-1]}, min-occ = {occ[0]}')
    print(f'99%-occ = {occ[int(len(occ) * 0.99)]}, 90%-occ={occ[int(len(occ) * 0.90)]} 1%-occ = {occ[int(len(occ) * 0.01)]}')
    print(f'99%-cumu-occ = {reduce(lambda x, y: x + y, occ[:int(len(occ) * 0.99)])}')
    print(f'90%-cumu-occ = {reduce(lambda x, y: x + y, occ[:int(len(occ) * 0.90)])}')
    print(f'median-occ = {occ[int(len(occ) * 0.5)]}')
    print(f'lock_ty = {lock_ty}, written = {len(written)}')


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print('Usage: trace_analyze.py <workload>')
        sys.exit(1)

    workload = sys.argv[1]
    main(workload)
