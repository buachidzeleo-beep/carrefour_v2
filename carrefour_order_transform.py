#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Carrefour Order Transformation
------------------------------
We do NOT modify incoming Carrefour order files. They are non-changeable. 
All adaptation is done on our side to output a correct ERP-upload format.

Steps implemented:
1) Delete columns by 1-based positions: 1,2,4,5,6,7,9,11
2) Move SUPNAM to the first column; move LPO to the last column
3) Group rows by (STR NAME, LPO); then arrange these blocks so that, whenever possible,
   no two consecutive blocks have the same STR NAME (to prevent ERP from merging them).

Usage:
    python carrefour_order_transform.py --input "Carrefour raw.xlsx" --output "Carrefour_transformed.xlsx"

Optional:
    --sheet "Sheet1"         # Input sheet name
    --keep-intermediate      # Also write the after Step 1&2 file next to the final output
    --engine "openpyxl"      # Pandas Excel engine
"""

import argparse
import sys
from pathlib import Path
from typing import List, Dict, Any

import pandas as pd


DROP_COLS_1BASED = [1, 2, 4, 5, 6, 7, 9, 11]
EXPECTED_AFTER = ["SUPNAM", "STR NAME", "BARCODE", "DESC", "QTYORD", "CP", "LPO"]


def step1_delete_columns(df: pd.DataFrame, drop_1based: List[int]) -> pd.DataFrame:
    """Delete columns by 1-based positions."""
    to_drop_0based = [i - 1 for i in drop_1based]
    to_keep = [i for i in range(df.shape[1]) if i not in to_drop_0based]
    return df.iloc[:, to_keep].copy()


def step2_reorder_supnam_lpo(df: pd.DataFrame) -> pd.DataFrame:
    """Move SUPNAM to first column and LPO to last column (if present)."""
    cols = list(df.columns)
    if "SUPNAM" in cols:
        cols.remove("SUPNAM")
        cols = ["SUPNAM"] + cols
    if "LPO" in cols:
        cols.remove("LPO")
        cols = cols + ["LPO"]
    return df[cols]


def _group_blocks(df: pd.DataFrame) -> List[Dict[str, Any]]:
    """
    Build blocks grouped by (STR NAME, LPO) preserving first appearance order.
    Each block is a dict with: store, lpo, first_idx, df (rows for that block).
    """
    # Preserve original row order
    df = df.reset_index(drop=False).rename(columns={"index": "_orig_idx"})
    seen = set()
    order_keys = []
    for _, row in df.iterrows():
        key = (row["STR NAME"], row["LPO"])
        if key not in seen:
            seen.add(key)
            order_keys.append(key)

    blocks = []
    for (store, lpo) in order_keys:
        blk = df[(df["STR NAME"] == store) & (df["LPO"] == lpo)].copy()
        first_idx = int(blk["_orig_idx"].min())
        blocks.append({
            "store": store,
            "lpo": lpo,
            "first_idx": first_idx,
            "df": blk.sort_values("_orig_idx").drop(columns=["_orig_idx"]).reset_index(drop=True)
        })
    return blocks


def _arrange_blocks_no_adjacent_same_store(blocks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Arrange blocks so that no two consecutive blocks have the same store, when possible.
    Greedy approach:
      - At each step, choose the earliest-first_idx block with store != last_store.
      - If none available, pick the earliest overall (unavoidable adjacency).
    """
    remaining = sorted(blocks, key=lambda b: b["first_idx"])
    arranged = []
    last_store = None
    while remaining:
        candidates = [b for b in remaining if b["store"] != last_store]
        if candidates:
            pick = sorted(candidates, key=lambda b: b["first_idx"])[0]
        else:
            pick = remaining[0]
        arranged.append(pick)
        last_store = pick["store"]
        remaining.remove(pick)
    return arranged


def step3_group_and_arrange(df: pd.DataFrame) -> pd.DataFrame:
    """Group by (STR NAME, LPO) and arrange groups to avoid adjacent same-store blocks."""
    # Sanity check: required columns
    for c in ["STR NAME", "LPO"]:
        if c not in df.columns:
            raise ValueError(f"Required column '{c}' is missing after Step 2. Found columns: {list(df.columns)}")

    blocks = _group_blocks(df)
    arranged = _arrange_blocks_no_adjacent_same_store(blocks)
    out_df = pd.concat([b["df"] for b in arranged], axis=0).reset_index(drop=True)
    return out_df


def transform(input_path: Path, output_path: Path, sheet: str = None, engine: str = "openpyxl",
              keep_intermediate: bool = False) -> None:
    # Read
    read_kwargs = {"engine": engine}
    if sheet:
        read_kwargs["sheet_name"] = sheet
    df = pd.read_excel(input_path, **read_kwargs)

    # Step 1
    df1 = step1_delete_columns(df, DROP_COLS_1BASED)

    # Step 2
    df2 = step2_reorder_supnam_lpo(df1)

    # Verify structure
    missing = [c for c in EXPECTED_AFTER if c not in df2.columns]
    if missing:
        raise ValueError(f"After Step 1&2, expected columns missing: {missing}. Got: {list(df2.columns)}")

    # Step 3 (v2 grouping)
    df3 = step3_group_and_arrange(df2)

    # Write outputs
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df3.to_excel(output_path, index=False, engine=engine)

    if keep_intermediate:
        inter_path = output_path.with_name(output_path.stem + "_after_step1_2" + output_path.suffix)
        df2.to_excel(inter_path, index=False, engine=engine)


def main(argv=None):
    parser = argparse.ArgumentParser(description="Transform Carrefour raw order file for ERP upload.")
    parser.add_argument("--input", "-i", required=True, help="Path to the raw Carrefour Excel file.")
    parser.add_argument("--output", "-o", required=True, help="Path to write the transformed Excel file.")
    parser.add_argument("--sheet", "-s", default=None, help="Input sheet name (optional).")
    parser.add_argument("--engine", default="openpyxl", help="Pandas Excel engine (default: openpyxl).")
    parser.add_argument("--keep-intermediate", action="store_true",
                        help="Also write the file after Step 1&2 (suffix _after_step1_2).")

    args = parser.parse_args(argv)

    input_path = Path(args.input)
    output_path = Path(args.output)

    # Principle: incoming order files are non-changeable; we adapt on our side.
    try:
        transform(input_path, output_path, sheet=args.sheet, engine=args.engine,
                  keep_intermediate=args.keep_intermediate)
        print(f"OK: wrote transformed file -> {output_path}")
        if args.keep_intermediate:
            print(f"OK: wrote intermediate (after Step 1&2) file next to output.")
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
