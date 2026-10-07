#!/usr/bin/env python3
import json
import os
import sys


def highest(root: str) -> int:
    ledger = os.path.join(root, "agent-001", "ledger")
    nums = []
    for name in os.listdir(ledger):
        if name.endswith(".json"):
            try:
                nums.append(int(name[:-5]))
            except ValueError:
                pass
    return max(nums) if nums else 0


def verify(root: str, total: int) -> None:
    ledger = os.path.join(root, "agent-001", "ledger")
    seen = []
    for name in os.listdir(ledger):
        if not name.endswith(".json"):
            continue
        n = int(name[:-5])
        with open(os.path.join(ledger, name), encoding="utf-8") as f:
            rec = json.load(f)
        if rec["task_id"] != n or rec["sequence"] != n:
            raise SystemExit(f"ledger_task_mismatch file={name}")
        seen.append(n)
    seen.sort()
    expected = list(range(1, total + 1))
    if seen != expected:
        raise SystemExit(
            f"ledger_not_contiguous first={seen[:10]} last={seen[-10:]} "
            f"count={len(seen)} expected={total}"
        )
    print(f"business_sequence=CONTIGUOUS 1..{total}")


if __name__ == "__main__":
    if len(sys.argv) == 2 and sys.argv[1] == "--highest":
        print(highest(sys.argv[2]))
    elif len(sys.argv) == 3 and sys.argv[1] == "--highest":
        print(highest(sys.argv[2]))
    elif len(sys.argv) == 3:
        verify(sys.argv[1], int(sys.argv[2]))
    else:
        raise SystemExit("usage: verify_sequence.py ROOT TOTAL | verify_sequence.py --highest ROOT")
