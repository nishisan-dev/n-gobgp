#!/usr/bin/env python3
"""Fail closed if any CI job fails, is skipped, or is missing from needs."""
import json
import os
from pathlib import Path
import re

workflow = Path('.github/workflows/ci.yml').read_text().split('jobs:', 1)[1]
expected = set(re.findall(r'^  ([a-zA-Z0-9_-]+):$', workflow, re.M)) - {'required'}
results = json.loads(os.environ['NEEDS_JSON'])
if not expected or set(results) != expected:
    raise SystemExit('required gate dependencies differ from workflow jobs')
failed = {job: result.get('result') for job, result in results.items()
          if result.get('result') != 'success'}
if failed:
    raise SystemExit(f'CI did not succeed: {failed}')
print(f'All {len(results)} CI jobs succeeded')
