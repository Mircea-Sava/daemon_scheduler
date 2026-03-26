"""Tests for WorkerSlotLimiter from sequencer."""

import sys
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from sequencer import WorkerSlotLimiter


# ---------------------------------------------------------------------------
# 1. Basic acquire / release
# ---------------------------------------------------------------------------

def test_basic_acquire_release():
    limiter = WorkerSlotLimiter(4)
    assert limiter.available_slots == 4

    limiter.acquire(1)
    assert limiter.available_slots == 3

    limiter.release(1)
    assert limiter.available_slots == 4


# ---------------------------------------------------------------------------
# 2. Acquire all slots
# ---------------------------------------------------------------------------

def test_acquire_all_slots():
    limiter = WorkerSlotLimiter(3)
    limiter.acquire(3)
    assert limiter.available_slots == 0


# ---------------------------------------------------------------------------
# 3. Release does not exceed total
# ---------------------------------------------------------------------------

def test_release_does_not_exceed_total():
    limiter = WorkerSlotLimiter(4)
    limiter.acquire(1)
    # Release more than we acquired
    limiter.release(3)
    assert limiter.available_slots == limiter.total_slots


# ---------------------------------------------------------------------------
# 4. Minimum total is one
# ---------------------------------------------------------------------------

def test_minimum_total_is_one():
    limiter = WorkerSlotLimiter(0)
    assert limiter.total_slots == 1
    assert limiter.available_slots == 1


# ---------------------------------------------------------------------------
# 5. Minimum acquire is one
# ---------------------------------------------------------------------------

def test_minimum_acquire_is_one():
    limiter = WorkerSlotLimiter(5)
    limiter.acquire(0)  # should acquire 1 slot (min clamp)
    assert limiter.available_slots == 4


# ---------------------------------------------------------------------------
# 6. Blocking acquire
# ---------------------------------------------------------------------------

def test_blocking_acquire():
    limiter = WorkerSlotLimiter(2)
    limiter.acquire(2)  # Thread A holds all slots

    result = {"blocked": True, "acquired": False}

    def thread_b():
        result["blocked"] = True
        limiter.acquire(1)  # should block until slots are freed
        result["acquired"] = True

    t = threading.Thread(target=thread_b)
    t.start()

    # Give thread B a moment to start and block
    time.sleep(0.1)
    assert result["acquired"] is False, "Thread B should still be blocked"

    # Release one slot so thread B can proceed
    limiter.release(1)
    t.join(timeout=2)

    assert result["acquired"] is True, "Thread B should have acquired after release"
    assert limiter.available_slots == 0  # thread B took the released slot


# ---------------------------------------------------------------------------
# 7. Concurrent acquire / release (no deadlock)
# ---------------------------------------------------------------------------

def test_concurrent_acquire_release():
    limiter = WorkerSlotLimiter(4)
    errors: list[str] = []

    def worker(slot_count: int, iterations: int):
        try:
            for _ in range(iterations):
                limiter.acquire(slot_count)
                time.sleep(0.001)
                limiter.release(slot_count)
        except Exception as exc:
            errors.append(str(exc))

    threads = [
        threading.Thread(target=worker, args=(1, 20)),
        threading.Thread(target=worker, args=(2, 15)),
        threading.Thread(target=worker, args=(1, 20)),
        threading.Thread(target=worker, args=(1, 20)),
    ]

    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10)

    assert not errors, f"Unexpected errors: {errors}"
    assert limiter.available_slots == limiter.total_slots, (
        "All slots should be returned after concurrent work"
    )
