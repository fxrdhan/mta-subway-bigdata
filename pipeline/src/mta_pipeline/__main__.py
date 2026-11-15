"""Daily batch ELT run: extract and load changed days, transform changed months, load the marts."""
from __future__ import annotations

import argparse
import logging
import sys

from . import db, ingest, lake, load, transform
from .config import Settings

log = logging.getLogger("mta_pipeline")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="mta-pipeline", description=__doc__)
    parser.add_argument("--limit-days", type=int, help="fetch only the N most recent changed days (tests)")
    parser.add_argument("--rebuild", action="store_true", help="rebuild every month from bronze, e.g. after a rule change")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    settings = Settings.from_env()
    pg = db.connect()
    db.apply_schema(pg)
    con = lake.connect(settings)
    run_id = db.start_run(pg, "batch-elt")
    days = rows = 0
    try:
        days, rows, months = ingest.run(settings, con, pg, args.limit_days)
        stored = sorted(db.stored_days(pg))
        if args.rebuild:
            months |= {d.replace(day=1) for d in stored}
        built = transform.run(settings, con, stored, months)
        load.run(settings, con, pg, built)
        db.finish_run(pg, run_id, "success", days, rows, [b.month for b in built])
        log.info("done: %d days fetched (%s rows), %d months built", days, f"{rows:,}", len(built))
        return 0
    except Exception as error:  # log the failure in pipeline_run, then fail the job
        db.finish_run(pg, run_id, "failed", days, rows, message=f"{type(error).__name__}: {error}"[:2000])
        log.exception("run failed")
        return 1


if __name__ == "__main__":
    sys.exit(main())
