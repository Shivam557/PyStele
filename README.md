

# PyStele Checkpointing v1 (Day 1)

### Install deps
```bash
pip install msgpack pytest


## Reading checkpoint metadata

Each checkpoint includes rich metadata for traceability.

```python
import json

with open("snapshots/<checkpoint_id>/metadata.json") as f:
    meta = json.load(f)

print(meta["caller"])        # file, function, line
print(meta["environment"])   # python_version, pid
print(meta["git_commit"])    # code version
