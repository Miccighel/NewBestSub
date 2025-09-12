# Package it.uniud.newbestsub.utils

Shared helpers and constants used across the project.

## Key types
- `Constants`  
  Centralizes paths (base/input/output/log), separators, run timestamp (`RUN_TIMESTAMP`), target/correlation ids, file-name tokens (suffixes), and streaming throttles.
- `Tools`  
  Small pure utilities:
  - `sanitizeForFile(part)` → safe filename token.
  - `folderBaseName(vararg parts)` → joined, sanitized token.
  - `buildParamsToken(...)` → stable parameter token (no timestamp).
  - `buildContainerFolderName(...)` → parameter token + `-time<RUN_TIMESTAMP>`.
  - `stableSeedFrom(Parameters)` → SHA-256–based deterministic `Long` seed.
- `LogManager`  
  Logging utilities that assume Log4j2 reads `baseLogFileName`:
  - `ensureLogDir()` → `<project>/log/`.
  - `getBootstrapLogFilePath()` → early “Execution<timestamp>.log”.
  - `buildFinalLogFilePathFromParams(token)` → parameterized path.
  - `switchActiveLogFile(path)` / `updateRootLoggerLevel(level)` / `promoteBootstrapToParameterizedLog(token, level?)`.
- `RandomBridge`  
  Deterministic RNG plumbing for jMetal:
  - `installDeterministic(seed)` → installs a `SplittableRandom` adapter.
  - `childSeed(label, index=0)` → derived stable seeds.
  - `withSeed(seed) { ... }` → scoped temporary RNG.

## Conventions
- Parameter tokens always include correlation and core dimensions, e.g.  
  `AH99-Pearson-topN-sysM-poP-iI[-rR][-mxM][-peA_B]`
- Container folders append the run marker:  
  `...-time<yyyy-MM-dd-HH-mm-ss>`
- Use `Locale.ROOT` when formatting numbers for CSV.
- Paths in `Constants` always end with the system `PATH_SEPARATOR`.

## Notes & tips
- The `baseLogFileName` system property must be set before Log4j2 initializes (bootstrap file), then switched to the parameterized file via `LogManager.promoteBootstrapToParameterizedLog(...)`.
- `RandomBridge.installDeterministic(seed)` guarantees reproducible streams **only** if the call order is identical between runs.
