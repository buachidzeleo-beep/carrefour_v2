# Carrefour Order Transformation

**Principle:** Incoming Carrefour order files are **non-changeable**. We adapt on our side to produce the exact ERP-upload format.

## What this script does

1. **Delete columns by position** (1-based): `1, 2, 4, 5, 6, 7, 9, 11`
2. **Reorder columns**:
   - Move `SUPNAM` to the **first** column
   - Move `LPO` to the **last** column
   - Final structure: `SUPNAM, STR NAME, BARCODE, DESC, QTYORD, CP, LPO`
3. **Group and arrange** to avoid ERP merging different orders:
   - Group by (`STR NAME`, `LPO`) so each group is one order block
   - Arrange groups so **no two consecutive groups share the same `STR NAME`** whenever possible

> We only **reorder rows**. We do **not** change values.

## Quick start

```bash
pip install -r requirements.txt

python carrefour_order_transform.py \
  --input "Carrefour raw.xlsx" \
  --output "Carrefour_transformed.xlsx" \
  --keep-intermediate
```

### Options
- `--sheet "Sheet1"`: specify input sheet name
- `--engine openpyxl`: pandas Excel engine (default: `openpyxl`)
- `--keep-intermediate`: write an extra file with the state after Step 1&2

## Why Step 3 matters
A single store (`STR NAME`) can have multiple `LPO` values on the same date/period. If adjacent in the file, the ERP may treat them as **one order**. By grouping and interleaving orders from different stores, we minimize same-store adjacency and prevent unintended merges. If only one store’s blocks remain at the end, a same-store adjacency can be unavoidable at that boundary.

## Notes
- Designed around the provided Carrefour sample structure.
- If your ERP requires a different ordering constraint, we can add a pluggable strategy without changing the file structure.


## Shop schedule filtering (Carrefour)

- Config file: `config/carrefour_shop_schedule.xlsx`
- Columns:
  - `STR NAME` — store name (must match order file)
  - `allowed_day` — integer 1–7 (1=Mon … 7=Sun)
- The Streamlit portal loads this file and splits the transformed orders into:
  - `ALLOWED` — store is allowed to receive on selected day
  - `WRONG DAY` — store is not allowed or missing in schedule
