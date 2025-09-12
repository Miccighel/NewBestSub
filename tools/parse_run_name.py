#!/usr/bin/env python3
"""
parse_run_name.py — parse NewBestSub run container folder names into structured data.

Overview
This helper decodes folder names produced by NewBestSub into a typed object so you can
programmatically inspect run parameters without re-deriving them from logs or configs.

Canonical pattern
<DATASET>-<CORR>-top<Topics>-sys<Systems>-po<Population>-i<Iterations>
[-r<Repetitions>][-exec<Executions>][-seed<seed>][-det]
-time<YYYY-MM-DD-HH-mm-ss>

Examples
AH99-Pearson-top50-sys129-po1000-i10000-r2000-time2025-08-22-19-19-40
My-DS-Kendall-top12-sys34-po200-i5000-exec5-seed123456-det-time2025-09-01-07-30-00

Notes
• Correlation is one of: Pearson, Kendall.
• Optional flags are truly optional and may appear or not; order is fixed as in the pattern.
• The “det” flag is represented as a boolean in the parsed result (True when present).
"""

from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass, asdict
from typing import Optional, Dict, Any

# Anchored, named-group regex that matches the exact container naming scheme.
# Groups:
#   dataset        free text up to the first '-' before correlation (non-greedy)
#   corr           'Pearson' or 'Kendall'
#   topics         integer after 'top'
#   systems        integer after 'sys'
#   population     integer after 'po'
#   iterations     integer after 'i'
#   repetitions    optional integer after '-r'
#   executions     optional integer after '-exec'
#   seed           optional integer after '-seed'
#   det            optional literal 'det' (presence-only flag)
#   timestamp      yyyy-MM-dd-HH-mm-ss after '-time'
RUN_RE = re.compile(
    r'^'
    r'(?P<dataset>.+?)-'
    r'(?P<corr>Pearson|Kendall)-'
    r'top(?P<topics>\d+)-'
    r'sys(?P<systems>\d+)-'
    r'po(?P<population>\d+)-'
    r'i(?P<iterations>\d+)'
    r'(?:-r(?P<repetitions>\d+))?'
    r'(?:-exec(?P<executions>\d+))?'
    r'(?:-seed(?P<seed>\d+))?'
    r'(?:-(?P<det>det))?'
    r'-time(?P<timestamp>\d{4}-\d{2}-\d{2}-\d{2}-\d{2}-\d{2})'
    r'$'
)


@dataclass
class RunName:
    """
    Structured representation of a NewBestSub run container name.

    Fields
    dataset          dataset identifier as it appeared in the name (may include dashes)
    correlation      'Pearson' or 'Kendall'
    topics           number of topics (int)
    systems          number of systems (int)
    population       population size (int)
    iterations       number of iterations (int)
    repetitions      optional repetitions (int) for Average/All modes
    executions       optional merged executions count (int)
    seed             optional master seed (int)
    deterministic    True if '-det' flag is present, else False
    timestamp        run timestamp string 'yyyy-MM-dd-HH-mm-ss'

    Conversion
    Use .to_dict() for JSON serialization; all numeric fields are standard Python ints.
    """
    dataset: str
    correlation: str
    topics: int
    systems: int
    population: int
    iterations: int
    repetitions: Optional[int] = None
    executions: Optional[int] = None
    seed: Optional[int] = None
    deterministic: bool = False
    timestamp: str = ""

    def to_dict(self) -> Dict[str, Any]:
        """Return a JSON-ready dictionary representation."""
        return asdict(self)


def parse_run_name(name: str) -> RunName:
    """
    Parse a container folder name into a RunName object.

    Behavior
    • Strips surrounding whitespace.
    • Validates the full name against the canonical pattern (anchored).
    • Converts known numeric fields to int.
    • Sets 'deterministic' to True when the '-det' flag is present.

    Errors
    Raises ValueError when the input does not match the expected format.

    Parameters
    name  container folder name (e.g., 'AH99-Pearson-top50-...-time2025-08-22-19-19-40')

    Returns
    RunName instance populated from the parsed components.
    """
    m = RUN_RE.match(name.strip())
    if not m:
        raise ValueError(f"Not a valid NewBestSub run name: {name!r}")
    gd = m.groupdict()
    return RunName(
        dataset=gd["dataset"],
        correlation=gd["corr"],
        topics=int(gd["topics"]),
        systems=int(gd["systems"]),
        population=int(gd["population"]),
        iterations=int(gd["iterations"]),
        repetitions=int(gd["repetitions"]) if gd.get("repetitions") else None,
        executions=int(gd["executions"]) if gd.get("executions") else None,
        seed=int(gd["seed"]) if gd.get("seed") else None,
        deterministic=bool(gd.get("det")),  # True when group matched
        timestamp=gd["timestamp"],
    )


def main(argv=None) -> None:
    """
    CLI entry point.

    Usage
      python parse_run_name.py "AH99-Pearson-top50-sys129-po1000-i10000-r2000-time2025-08-22-19-19-40"
      python parse_run_name.py --pretty "My-DS-Kendall-top12-sys34-po200-i5000-exec5-seed42-det-time2025-09-01-07-30-00"

    Options
      --pretty   pretty-print the resulting JSON with indentation.

    Exit codes
      0 on success, non-zero on invalid input (ValueError propagates).
    """
    ap = argparse.ArgumentParser(
        description="Parse a NewBestSub run container name into a JSON object."
    )
    ap.add_argument("name", help="Run container name (folder).")
    ap.add_argument("--pretty", action="store_true", help="Pretty-print JSON.")
    ns = ap.parse_args(argv)

    run = parse_run_name(ns.name)
    if ns.pretty:
        print(json.dumps(run.to_dict(), indent=2))
    else:
        print(json.dumps(run.to_dict(), separators=(",", ":")))


if __name__ == "__main__":
    main()
