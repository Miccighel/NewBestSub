# Package it.uniud.newbestsub.program

CLI entrypoint and runtime glue.

## Responsibilities
- Parse options (dataset, target, correlation, iterations, reps, population, percentiles).
- Configure logging and final log path from parameter tokens.
- Drive `DatasetController.load` and `DatasetModel.solve`.

## Notes
- Logs are parameterized with a token and run timestamp.
- Recommend `-XX:-UseGCOverheadLimit` for large runs if memory pressure occurs.
