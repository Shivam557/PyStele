import re
from pystele.core.ids import new_execution_id, new_run_id, new_branch_id

REGEX = re.compile(r"^[a-z]+-\d{8}T\d{6}-\w{8}$")


def test_id_format_and_uniqueness():
    ids = set()
    for _ in range(1000):
        for fn in (new_execution_id, new_run_id, new_branch_id):
            v = fn()
            assert REGEX.match(v)
            ids.add(v)
    assert len(ids) == 3000
