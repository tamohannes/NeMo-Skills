#!/usr/bin/env python3
"""
Streaming merge of default.jsonl with output.jsonl.

Creates two outputs:
  • artsiv_w_findings.jsonl  – adds both locations + findings
  • artsiv_w_locations.jsonl – adds only locations
Includes tqdm progress bars and per-line timing diagnostics.
"""

import argparse
import time
from pathlib import Path
import orjson
from tqdm import tqdm


def build_lookup_streaming(output_path: Path):
    """Read output.jsonl line by line, store only needed fields."""
    print(f"\n🔹 Building lookup from {output_path} (streaming mode)")
    lookup = {}

    file_size = output_path.stat().st_size
    start_time = time.time()

    with open(output_path, "rb") as f, tqdm(
        total=file_size,
        unit="B",
        unit_scale=True,
        desc="Loading output.jsonl",
        dynamic_ncols=True,
    ) as pbar:
        for i, line in enumerate(f, 1):
            pbar.update(len(line))
            if not line.strip():
                continue

            t0 = time.time()
            try:
                rec = orjson.loads(line)
            except Exception as e:
                print(f"⚠️ Failed to parse line {i}: {e}")
                continue

            elapsed = time.time() - t0
            if elapsed > 1.0:
                print(f"⚠️ Slow parse ({elapsed:.2f}s) at line {i}")

            iid = rec.get("instance_id")
            if not iid:
                continue

            lookup[iid] = {
                "locations": rec.get("locations"),
                "findings": rec.get("findings"),
            }

    print(f"✅ Built lookup for {len(lookup)} instances in {time.time() - start_time:.2f}s")
    return lookup


def merge_files(default_path: Path, output_path: Path, out_with_findings: Path, out_locations_only: Path):
    print("\n🚀 Starting streaming merge...")
    print(f"  Default file: {default_path}")
    print(f"  Output file:  {output_path}")
    print(f"  Targets:      {out_with_findings}, {out_locations_only}")

    lookup = build_lookup_streaming(output_path)

    total_lines = sum(1 for _ in open(default_path, "rb"))
    print(f"🔹 Merging {total_lines} records...")

    merged_count = 0
    found_locs = found_findings = 0
    start_time = time.time()

    with open(default_path, "rb") as fin, \
         open(out_with_findings, "wb") as fout_full, \
         open(out_locations_only, "wb") as fout_loc, \
         tqdm(total=total_lines, desc="Merging records", unit="lines", dynamic_ncols=True) as pbar:

        for line in fin:
            if not line.strip():
                continue

            rec = orjson.loads(line)
            iid = rec.get("instance_id")
            if not iid:
                continue

            extra = lookup.get(iid)
            if extra:
                if extra.get("locations") is not None:
                    rec["locations"] = extra["locations"]
                    found_locs += 1
                if extra.get("findings") is not None:
                    rec["findings"] = extra["findings"]
                    found_findings += 1

            # Write version with both fields
            fout_full.write(orjson.dumps(rec))
            fout_full.write(b"\n")

            # Write version without findings
            rec_no_findings = dict(rec)
            rec_no_findings.pop("findings", None)
            fout_loc.write(orjson.dumps(rec_no_findings))
            fout_loc.write(b"\n")

            merged_count += 1
            pbar.update(1)

    print(f"\n✅ Merge complete in {time.time() - start_time:.2f}s")
    print(f"  Total records: {merged_count}")
    print(f"  Added locations: {found_locs}")
    print(f"  Added findings:  {found_findings}")
    print(f"  Outputs written to:\n    {out_with_findings}\n    {out_locations_only}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset_dir", type=str, required=True)
    parser.add_argument("--output_jsonl", type=str, required=True)
    args = parser.parse_args()

    dataset_dir = Path(args.dataset_dir)
    default_path = dataset_dir / "default.jsonl"
    output_path = Path(args.output_jsonl)

    out_with_findings = dataset_dir / "artsiv_w_findings.jsonl"
    out_locations_only = dataset_dir / "artsiv_w_locations.jsonl"

    merge_files(default_path, output_path, out_with_findings, out_locations_only)


if __name__ == "__main__":
    main()
