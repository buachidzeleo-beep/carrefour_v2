# streamlit app: Carrefour Order Portal (STR-first schedule filtering)
# Principle: incoming Carrefour order files are NON-changeable.
# We adapt on our side to upload in proper ERP format.

import io
from typing import List, Dict, Any, Tuple

import pandas as pd
import streamlit as st


st.set_page_config(page_title="Carrefour Order Transformer", layout="wide")

st.title("Carrefour Order Transformer")
st.caption("Incoming Carrefour orders are non-changeable; all adaptation is done on our side for ERP upload.")

# Step 1 & 2 config
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


# ---------- Core helpers ----------

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
    Greedy:
      - At each step, choose the earliest-first_idx block with store != last_store.
      - If none, pick earliest overall (adjacency unavoidable).
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


def load_schedule_str(path: str) -> pd.DataFrame:
    """
    Load shop schedule from Excel.
    Expected columns: STR, allowed_day (1–7).
    Matching is case-insensitive on STR code.
    """
    df = pd.read_excel(path)

    cols_norm = {c: c.strip() for c in df.columns}
    df = df.rename(columns=cols_norm)

    required = ["STR", "allowed_day"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(
            f"Shop schedule (STR-based) missing required columns: {missing}. Found: {list(df.columns)}"
        )

    df["allowed_day"] = pd.to_numeric(df["allowed_day"], errors="coerce")
    df = df[df["allowed_day"].between(1, 7)]

    df["STR_KEY"] = df["STR"].astype(str).str.strip().str.upper()

    return df


def filter_raw_by_str_day(
    raw_df: pd.DataFrame,
    schedule_df: pd.DataFrame,
    selected_day: int,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Filter RAW orders by STR code and allowed_day BEFORE any other steps.

    raw_df: original Carrefour raw (we do NOT modify it in-place).
    Schedule: columns STR, allowed_day (1–7).
    selected_day: int 1–7.

    Returns:
      raw_allowed, raw_wrong  (both still in raw-format structure).
    """
    df = raw_df.copy()

    # Determine where STR code comes from:
    #  - if there is an explicit 'STR' column — use it
    #  - else assume 2nd column (index 1) is STR code (Gxx)
    if "STR" in df.columns:
        df["STR_TMP"] = df["STR"].astype(str)
    else:
        if df.shape[1] < 2:
            raise ValueError("Raw file has fewer than 2 columns; cannot infer STR from column 2.")
        df["STR_TMP"] = df.iloc[:, 1].astype(str)

    df["STR_KEY"] = df["STR_TMP"].str.strip().str.upper()

    # Build schedule map
    sched_map: Dict[str, set] = {}
    for _, row in schedule_df.iterrows():
        key = row["STR_KEY"]
        day = row["allowed_day"]
        if pd.isna(day):
            continue
        day = int(day)
        sched_map.setdefault(key, set()).add(day)

    def is_allowed(str_key: str) -> bool:
        days = sched_map.get(str_key, None)
        if not days:
            return False
        return selected_day in days

    df["ALLOWED_DAY"] = df["STR_KEY"].apply(is_allowed)

    raw_allowed = df[df["ALLOWED_DAY"]].drop(columns=["STR_TMP", "STR_KEY", "ALLOWED_DAY"])
    raw_wrong = df[~df["ALLOWED_DAY"]].drop(columns=["STR_TMP", "STR_KEY", "ALLOWED_DAY"])

    return raw_allowed.reset_index(drop=True), raw_wrong.reset_index(drop=True)


def run_full_transform(raw_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, List[Dict[str, Any]]]:
    """
    Apply Steps 1–3 to a given RAW subset (no STR logic here).
    Returns: (df_after_step1_2, df_final_step3, blocks_meta)
    """
    df1 = step1_delete_columns(raw_df, DROP_COLS_1BASED)
    df2 = step2_reorder_supnam_lpo(df1)

    missing = [c for c in EXPECTED_AFTER if c not in df2.columns]
    if missing:
        raise ValueError(f"After Step 1&2, expected columns missing: {missing}. Found: {list(df2.columns)}")

    df3, blocks = step3_group_and_arrange(df2)
    return df2, df3, blocks


def to_excel_download(df: pd.DataFrame) -> bytes:
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Orders")
    buf.seek(0)
    return buf.read()


# ---------- UI ----------

with st.expander("Instructions", expanded=True):
    st.markdown(
        """
1. Upload the **Carrefour raw** Excel file.
2. Select delivery day (1–7).
3. The app will:
   - Load STR-based schedule from `config/carrefour_shop_schedule.xlsx`
   - **Immediately filter RAW orders by STR & allowed_day**
   - On the **allowed** subset:
        - **Step 1:** delete columns 1,2,4,5,6,7,9,11  
        - **Step 2:** reorder columns so `SUPNAM` is first and `LPO` is last  
        - **Step 3:** group by (`STR NAME`, `LPO`) and arrange to avoid adjacent same-store blocks
   - On the **wrong-day** subset: apply those same Steps 1–3 separately.
4. Outputs:
   - Transformed ALLOWED orders (for ERP upload)
   - Transformed WRONG-DAY orders (for correction)
   - Optionally, full transformed file ignoring schedule.
        """
    )

selected_day = st.selectbox(
    "Select delivery day (1=Mon … 7=Sun)",
    options=list(DAY_LABELS.keys()),
    format_func=lambda x: f"{x} — {DAY_LABELS.get(x, '?')}",
)

uploaded = st.file_uploader("Upload Carrefour raw.xlsx", type=["xlsx"])
keep_full_global = st.checkbox("Also produce FULL transformed file (ignore schedule)", value=False)

if uploaded is not None:
    try:
        raw_df = pd.read_excel(uploaded)
        st.subheader("Raw file (head)")
        st.dataframe(raw_df.head(20), use_container_width=True)

        # Load STR-based schedule
        try:
            schedule_df = load_schedule_str(CONFIG_SCHEDULE_PATH)
        except Exception as e:
            st.error(f"Could not load STR-based shop schedule from '{CONFIG_SCHEDULE_PATH}': {e}")
            st.stop()

        st.info(
            f"Using STR-based schedule from '{CONFIG_SCHEDULE_PATH}'. "
            f"Required columns: STR, allowed_day (1–7). Matching is case-insensitive on STR."
        )

        # --- 1. Filter raw by STR & day BEFORE any other transformation ---
        raw_allowed, raw_wrong = filter_raw_by_str_day(raw_df, schedule_df, selected_day)

        st.markdown("### Split by STR & allowed_day (on RAW level)")
        col_a, col_b = st.columns(2)
        with col_a:
            st.markdown("**ALLOWED RAW** (by STR & day)")
            st.write(f"Rows: {len(raw_allowed)}")
            st.dataframe(raw_allowed.head(20), use_container_width=True)
        with col_b:
            st.markdown("**WRONG-DAY RAW** (by STR & day)")
            st.write(f"Rows: {len(raw_wrong)}")
            st.dataframe(raw_wrong.head(20), use_container_width=True)

        # --- 2. Transform ALLOWED subset (Steps 1–3) ---
        st.markdown("---")
        st.subheader("Transform ALLOWED subset (Steps 1–3)")

        if len(raw_allowed) == 0:
            st.warning("No rows in ALLOWED subset for the selected day.")
            df2_allowed = pd.DataFrame()
            df3_allowed = pd.DataFrame()
            blocks_allowed = []
        else:
            try:
                df2_allowed, df3_allowed, blocks_allowed = run_full_transform(raw_allowed)
            except Exception as e:
                st.error(f"Error while transforming ALLOWED subset: {e}")
                df2_allowed, df3_allowed, blocks_allowed = pd.DataFrame(), pd.DataFrame(), []

        if not df3_allowed.empty:
            block_order_allowed = pd.DataFrame(
                [{"#": i + 1, "STR NAME": b["store"], "LPO": b["lpo"], "first_row_index": b["first_idx"]}
                 for i, b in enumerate(blocks_allowed)]
            )
            st.markdown("**ALLOWED — block order (STR NAME, LPO)**")
            st.dataframe(block_order_allowed, use_container_width=True, height=300)

            st.markdown("**ALLOWED — final transformed (head)**")
            st.dataframe(df3_allowed.head(50), use_container_width=True)

            st.download_button(
                label="Download ALLOWED — transformed (Steps 1–3)",
                data=to_excel_download(df3_allowed),
                file_name="Carrefour_transformed_ALLOWED.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

        # --- 3. Transform WRONG-DAY subset (Steps 1–3) ---
        st.markdown("---")
        st.subheader("Transform WRONG-DAY subset (Steps 1–3)")

        if len(raw_wrong) == 0:
            st.info("No WRONG-DAY rows; everything fits the schedule for selected day.")
            df2_wrong = pd.DataFrame()
            df3_wrong = pd.DataFrame()
            blocks_wrong = []
        else:
            try:
                df2_wrong, df3_wrong, blocks_wrong = run_full_transform(raw_wrong)
            except Exception as e:
                st.error(f"Error while transforming WRONG-DAY subset: {e}")
                df2_wrong, df3_wrong, blocks_wrong = pd.DataFrame(), pd.DataFrame(), []

        if not df3_wrong.empty:
            block_order_wrong = pd.DataFrame(
                [{"#": i + 1, "STR NAME": b["store"], "LPO": b["lpo"], "first_row_index": b["first_idx"]}
                 for i, b in enumerate(blocks_wrong)]
            )
            st.markdown("**WRONG-DAY — block order (STR NAME, LPO)**")
            st.dataframe(block_order_wrong, use_container_width=True, height=300)

            st.markdown("**WRONG-DAY — final transformed (head)**")
            st.dataframe(df3_wrong.head(50), use_container_width=True)

            st.download_button(
                label="Download WRONG-DAY — transformed (Steps 1–3)",
                data=to_excel_download(df3_wrong),
                file_name="Carrefour_transformed_WRONG_DAY.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

        # --- 4. Optional: full transformed file ignoring schedule ---
        if keep_full_global:
            st.markdown("---")
            st.subheader("FULL transformed file (ignore schedule, apply Steps 1–3 on all RAW)")

            try:
                df2_full, df3_full, blocks_full = run_full_transform(raw_df)
            except Exception as e:
                st.error(f"Error while transforming FULL RAW file: {e}")
                df3_full = pd.DataFrame()
            else:
                st.markdown("**FULL — final transformed (head)**")
                st.dataframe(df3_full.head(50), use_container_width=True)

                st.download_button(
                    label="Download FULL transformed (Steps 1–3, ignore schedule)",
                    data=to_excel_download(df3_full),
                    file_name="Carrefour_transformed_FULL_ignore_schedule.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )

        st.success("Completed: STR-based filtering on RAW first, then Steps 1–3 for allowed and wrong-day subsets.")

    except Exception as e:
        st.error(f"Error: {e}")
else:
    st.info("Upload the raw Carrefour order file to begin.")
