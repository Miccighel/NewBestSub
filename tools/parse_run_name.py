#!/usr/bin/env python3
"""
parse_run_name.py — helper to parse NewBestSub run container folder names.

New canonical pattern
---------------------
<DATASET>-<CORR>-top<Topics>-sys<Systems>-po<Population>-i<Iterations>[-r<Repetitions>][-exec<Executions>][-seed<seed>][-det]-time<YYYY-MM-DD-HH-mm-ss>

Example
-------
AH99-Pearson-top50-sys129-po1000-i10000-r2000-time2025-08-22-19-19-40
"""
from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass, asdict
from typing import Optional, Dict, Any, List

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
        d = asdict(self)
        return d

def parse_run_name(name: str) -> RunName:
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
        deterministic=bool(gd.get("det")),
        timestamp=gd["timestamp"],
    )

def main(argv=None):
    ap = argparse.ArgumentParser(description="Parse a NewBestSub run container name into a JSON object.")
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
