# Package it.uniud.newbestsub.utils

Shared helpers and constants.

## Key types
- `Constants`: well‑known strings (targets, correlation ids), run timestamp marker.
- `Tools`: helpers like parameter token builders and small utilities.
- `LogManager` / `RandomBridge`: logging configuration and RNG plumbing.

## Conventions
- Parameter tokens include correlation and core dimensions: `AH99-Pearson-topN-sysM-poP-iI-...`.
- Use `Locale.ROOT` for decimal formatting in CSV to avoid locale issues.
