"""
Runs the full medallion pipeline end-to-end: Bronze ingest -> Silver
clean/validate -> Gold star schema build -> sample analytical queries ->
print the Gold tables so you can see the model.

Run: python3 run_pipeline.py
"""
import subprocess
import sys


def run(cmd, label):
    print(f"\n{'=' * 70}\n{label}\n{'=' * 70}")
    result = subprocess.run([sys.executable] + cmd, capture_output=False)
    if result.returncode != 0:
        print(f"FAILED: {label}")
        sys.exit(result.returncode)


def main():
    run(["bronze_ingest.py"], "STEP 1: Bronze ingest (raw CSVs -> Delta, append-only, idempotent)")
    run(["silver_transform.py"], "STEP 2: Silver transform (clean, validate, quarantine, dedupe)")
    run(["gold_star_schema.py"], "STEP 3: Gold star schema build (dimensions + facts)")
    run(["query_gold.py"], "STEP 4: Sample analytical queries against the Gold star schema")
    run(["view_gold.py"], "STEP 5: Print Gold tables (columns + sample rows) so you can see the model")

    print("\nPipeline complete. Explore the Delta tables under ../lake/{bronze,silver,gold}")


if __name__ == "__main__":
    main()