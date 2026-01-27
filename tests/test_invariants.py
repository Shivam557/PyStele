from pystele.core.invariants import check_invariants


def test_failing_state():
    assert check_invariants({})


def test_passing_state():
    state = {
        "commit_log": [],
        "ids_mutable": False,
        "snapshots_consistent": True,
        "clock_monotonic": True,
        "version": "0.1",
        "storage_path": "/tmp",
    }
    assert check_invariants(state) == []
