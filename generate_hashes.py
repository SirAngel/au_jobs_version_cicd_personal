#!/usr/bin/env python3
import json
import hashlib
from pathlib import Path

hashes = {}
for job_file in Path('domains').rglob('*.py'):
    if job_file.is_file():
        with open(job_file, 'rb') as f:
            file_hash = hashlib.sha256(f.read()).hexdigest()
        hashes[str(job_file)] = file_hash

with open('/tmp/current_hashes.json', 'w') as f:
    json.dump(hashes, f, indent=2)

print(f"Estado generado con {len(hashes)} jobs")
