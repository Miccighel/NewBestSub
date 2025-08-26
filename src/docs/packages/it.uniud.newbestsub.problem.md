# Package it.uniud.newbestsub.problem

Search space, objective evaluation, and operators for NSGA‑II.

## Key types
- `BestSubsetProblem`: evaluates objectives with a pluggable correlation strategy and target encoder.
- `BestSubsetSolution`: binary mask over topics; tracks `numberOfSelectedTopics`, capacity.
- Operators (e.g., `BitFlipMutation`): mutation with invariants (keep ≥ 1 selected unless forced).

## Invariants
- Masks sized to `numberOfTopics`.
- Cardinality changes by ±1 for a single bit flip.
- Optional `forcedCardinality` constrains masks exactly.
