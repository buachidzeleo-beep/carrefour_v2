# streamlit app: Carrefour Order Portal (STR-based schedule filtering)
# Principle: incoming Carrefour order files are NON-changeable. We adapt on our side to upload in proper ERP format.

import io
from typing import List, Dict, Any, Tuple

import pandas as pd
import streamlit as st


st.set_page_config(page_title="Carrefour Order Transformer", layout="wide")

st.title("Carrefour Order Transformer")
st.caption("Incoming Carrefour orders are **non-changeable**; all adaptation occurs on our side for ERP upload.")

# Step 1 & 2 config
# NOTE: we keep logical deletion of raw column 2, but BEFORE that we copy it into a new 'STR' column
DROP_COLS_1BASED = [1, 2, 4, 5, 6, 7, 9, 11]
EXPECTED_AFTER = ["SUPNAM", "STR NAME", "BARCODE", "DESC", "QTYORD", "CP", "LPO"]

# Shop schedule config (by STR code, e.g. G28)
CONFIG_SCHEDULE_PATH = "config/carrefour_shop_schedule.xlsx"

DAY_LABELS = {
    1: "Monday",
    2: "Tuesday",
    3: "Wednesday",
    4: "Thursday",
    5: "Friday",
    6: "Saturday",
    7: "Sunday",
}


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
    df = df.reset_index(drop=False).rename(columns={"index": "_orig_idx"})
    seen = set()
    order_keys = []
    for _, row in df.iterrows():
        key = (row["STR NAME"], row["LPO"])
        if key not in seen:
            seen.add(key)
            order_keys.append(key)

    blocks: List[Dict[str, Any]] = []
    for (store, lpo) in order_keys:
        blk = df[(df["STR NAME"] == store) & (df["LPO"] == lpo)].copy()
        first_idx = int(blk["_orig_idx"].min())
        blocks.append({
            "store": store,
            "lpo": lpo,
            "first_idx": first_idx,
            "df": blk.sort_values("_orig_idx").drop(columns=["_orig_idx"]).reset_index(drop=True),
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
    arranged: List[Dict[str, Any]] = []
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


def step3_group_and_arrange(df: pd.DataFrame) -> Tuple[pd.DataFrame, List[Dict[str, Any]]]:
    """Group by (STR NAME, LPO) and arrange groups to avoid adjacent same-store blocks."""
    for c in ["STR NAME", "LPO"]:
        if c not in df.columns:
            raise ValueError(f"Required column '{c}' is missing after Step 2. Found columns: {list(df.columns)}")

    blocks = _group_blocks(df)
    arranged = _arrange_blocks_no_adjacent_same_store(blocks)
    out_df = pd.concat([b["df"] for b in arranged], axis=0).reset_index(drop=True)
    return out_df, arranged


def load_shop_schedule_by_str(path: str) -> pd.DataFrame:
    """
    Load shop schedule from Excel.
    Expected columns: STR, allowed_day (1–7).
    Matching is case-insensitive on STR code.
    """
    df = pd.read_excel(path)

    # Normalize column names
    cols_norm = {c: c.strip() for c in df.columns}
    df = df.rename(columns=cols_norm)

    required = ["STR", "allowed_day"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(
            f"Shop schedule (STR-based) missing required columns: {missing}. Found: {list(df.columns)}"
        )

    # Normalize allowed_day to numeric 1–7
    df["allowed_day"] = pd.to_numeric(df["allowed_day"], errors="coerce")
    df = df[df["allowed_day"].between(1, 7)]

    # Normalized STR key
    df["STR_KEY"] = df["STR"].astype(str).str.strip().str.upper()

    return df


def apply_schedule_filter_by_str(
    df_orders: pd.DataFrame,
    schedule_df: pd.DataFrame,
    selected_day: int,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Apply day-based filtering using STR code (e.g. G28).

    df_orders: dataframe AFTER Steps 1–3, must contain column STR (copied from raw second column).
    schedule_df: output of load_shop_schedule_by_str(...)
    selected_day: int 1–7

    Returns: (allowed_df, wrong_df), both WITHOUT the technical STR column for ERP export.
    """
    # Build schedule map: STR_KEY -> set of allowed days
    sched_map: Dict[str, set] = {}
    for _, row in schedule_df.iterrows():
        key = row["STR_KEY"]
        day = row["allowed_day"]
        if pd.isna(day):
            continue
        day = int(day)
        sched_map.setdefault(key, set()).add(day)

    df = df_orders.copy()

    if "STR" not in df.columns:
        raise ValueError("Orders dataframe is missing 'STR' column for STR-based filtering.")

    # Build normalized STR key on orders side
    df["STR_KEY"] = df["STR"].astype(str).str.strip().str.upper()

    def is_allowed(str_key: str) -> bool:
        days = sched_map.get(str_key, None)
        if not days:
            return False
        return selected_day in days

    df["ALLOWED_DAY"] = df["STR_KEY"].apply(is_allowed)

    # We drop STR and STR_KEY from export; they are technical only
    allowed_df = df[df["ALLOWED_DAY"]].drop(columns=["STR", "STR_KEY"]).reset_index(drop=True)
    wrong_df = df[~df["ALLOWED_DAY"]].drop(columns=["STR", "STR_KEY"]).reset_index(drop=True)

    return allowed_df, wrong_df


def to_excel_download(df: pd.DataFrame) -> bytes:
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Orders")
    buf.seek(0)
    return buf.read()


with st.expander("Instructions", expanded=True):
    st.markdown(
        """
1. Upload the **Carrefour raw** Excel file.
2. The app will:
   - **Step 1:** copy the `STR` code from raw column 2 into a technical `STR` column
     and then delete columns **1,2,4,5,6,7,9,11**  
   - **Step 2:** reorder columns so `SUPNAM` is first and `LPO` is last  
   - **Step 3:** group by (`STR NAME`, `LPO`) and arrange blocks to avoid **adjacent same-store** groups where possible.
3. Then it applies **day-based filtering** using `config/carrefour_shop_schedule.xlsx`:
   - schedule columns: `STR` (shop code like G28), `allowed_day` (1–7)
   - matching is case-insensitive on STR code
4. Preview results and download:
   - Full transformed file (Steps 1–3)
   - Allowed / Wrong-Day files for the selected weekday.
        """
    )

uploaded = st.file_uploader("Upload Carrefour raw.xlsx", type=["xlsx"])

keep_intermediate = st.checkbox("Also produce intermediate (after Step 1 & 2)", value=True)

if uploaded is not None:
    try:
        raw_df = pd.read_excel(uploaded)
        st.subheader("Raw file (head)")
        st.dataframe(raw_df.head(20), use_container_width=True)

        # Copy STR code from raw *before* dropping column 2.
        # Assumption: raw column 2 (1-based) contains STR code.
        # We store it into a technical 'STR' column for downstream use.
        if raw_df.shape[1] < 2:
            st.error("Raw file has fewer than 2 columns; cannot extract STR code from column 2.")
        else:
            raw_df["STR"] = raw_df.iloc[:, 1]

            # Step 1 & 2
            df1 = step1_delete_columns(raw_df, DROP_COLS_1BASED)
            df2 = step2_reorder_supnam_lpo(df1)

            # Validate columns
            missing = [c for c in EXPECTED_AFTER if c not in df2.columns]
            if missing:
                st.error(f"After Step 1&2, expected columns missing: {missing}. Found: {list(df2.columns)}")
            else:
                st.subheader("After Step 1 & 2 (head)")
                st.dataframe(df2.head(20), use_container_width=True)

                # Step 3 (group & arrange)
                df3, arranged_blocks = step3_group_and_arrange(df2)

                # Block order preview
                block_order_df = pd.DataFrame(
                    [{"#": i + 1, "STR NAME": b["store"], "LPO": b["lpo"], "first_row_index": b["first_idx"]}
                     for i, b in enumerate(arranged_blocks)]
                )
                st.subheader("Order of (STR NAME, LPO) blocks")
                st.dataframe(block_order_df, use_container_width=True, height=300)

                # --- Schedule-based day filtering (STR-based) ---
                st.markdown("---")
                st.subheader("Day-based filtering by STR code schedule")

                schedule_df = None
                schedule_error = None
                try:
                    schedule_df = load_shop_schedule_by_str(CONFIG_SCHEDULE_PATH)
                except Exception as e:
                    schedule_error = str(e)

                if schedule_error:
                    st.error(
                        f"Could not load STR-based shop schedule from '{CONFIG_SCHEDULE_PATH}': {schedule_error}"
                    )
                else:
                    st.info(
                        f"Using STR-based shop schedule from '{CONFIG_SCHEDULE_PATH}'. "
                        f"Columns: STR, allowed_day (1–7). Matching is case-insensitive on STR."
                    )

                    # Day selector
                    selected_day = st.selectbox(
                        "Select delivery day (1=Mon … 7=Sun)",
                        options=list(DAY_LABELS.keys()),
                        format_func=lambda x: f"{x} — {DAY_LABELS.get(x, '?')}",
                    )

                    # Apply filter using STR code
                    allowed_df, wrong_df = apply_schedule_filter_by_str(df3, schedule_df, selected_day)

                    col1, col2 = st.columns(2)
                    with col1:
                        st.markdown("**Allowed for selected day**")
                        st.write(f"Rows: {len(allowed_df)}")
                        st.dataframe(allowed_df.head(50), use_container_width=True)
                    with col2:
                        st.markdown("**WRONG DAY (not allowed for selected day)**")
                        st.write(f"Rows: {len(wrong_df)}")
                        st.dataframe(wrong_df.head(50), use_container_width=True)

                    st.markdown("Download by day (STR-based):")
                    c1, c2 = st.columns(2)
                    with c1:
                        st.download_button(
                            label="Download ALLOWED (by day, STR-based)",
                            data=to_excel_download(allowed_df),
                            file_name="Carrefour_transformed_ALLOWED.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        )
                    with c2:
                        st.download_button(
                            label="Download WRONG DAY (STR-based)",
                            data=to_excel_download(wrong_df),
                            file_name="Carrefour_transformed_WRONG_DAY.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        )

                st.subheader("Final output (head) — Steps 1–3 (no day filter)")
                # For ERP-export preview, we hide the technical STR column here as well
                df3_preview = df3.drop(columns=["STR"]) if "STR" in df3.columns else df3
                st.dataframe(df3_preview.head(50), use_container_width=True)

                # Downloads for full transformed files (without STR)
                export_df3 = df3.drop(columns=["STR"]) if "STR" in df3.columns else df3
                export_df2 = df2.drop(columns=["STR"]) if "STR" in df2.columns else df2

                st.download_button(
                    label="Download TRANSFORMED (Steps 1–3, no STR)",
                    data=to_excel_download(export_df3),
                    file_name="Carrefour_transformed.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )

                if keep_intermediate:
                    st.download_button(
                        label="Download INTERMEDIATE (after Step 1 & 2, no STR)",
                        data=to_excel_download(export_df2),
                        file_name="Carrefour_after_step1_2.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    )

                st.success(
                    "Transformation complete. Raw input is not modified. We copy STR code for internal filtering, "
                    "then drop it from ERP exports. Day-based filtering is done strictly by STR code."
                )

    except Exception as e:
        st.error(f"Error: {e}")
else:
    st.info("Upload the raw Carrefour order file to begin.")
