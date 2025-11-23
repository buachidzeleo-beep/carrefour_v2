# streamlit app: Carrefour Order Portal
# Principle: incoming Carrefour order files are NON-changeable. We adapt on our side to upload in proper ERP format.

import io
from typing import List, Dict, Any

import pandas as pd
import streamlit as st


st.set_page_config(page_title="Carrefour Order Transformer", layout="wide")

st.title("Carrefour Order Transformer")
st.caption("Incoming Carrefour orders are **non-changeable**; all adaptation occurs on our side for ERP upload.")

DROP_COLS_1BASED = [1, 2, 4, 5, 6, 7, 9, 11]
EXPECTED_AFTER = ["SUPNAM", "STR NAME", "BARCODE", "DESC", "QTYORD", "CP", "LPO"]
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


def load_shop_schedule(path: str) -> pd.DataFrame:
    """Load shop schedule from Excel. Expected columns: STR NAME, allowed_day (1–7).
    Multiple rows per STR NAME are allowed (multiple delivery days)."""
    df = pd.read_excel(path)
    # Normalize columns
    cols_norm = {c: c.strip() for c in df.columns}
    df = df.rename(columns=cols_norm)
    required = ["STR NAME", "allowed_day"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Shop schedule missing required columns: {missing}. Found: {list(df.columns)}")
    # Ensure allowed_day numeric 1–7
    df["allowed_day"] = pd.to_numeric(df["allowed_day"], errors="coerce")
    df = df[df["allowed_day"].between(1, 7)]
    return df


EXPECTED_AFTER = ["SUPNAM", "STR NAME", "BARCODE", "DESC", "QTYORD", "CP", "LPO"]

def step1_delete_columns(df: pd.DataFrame, drop_1based: List[int]) -> pd.DataFrame:
    to_drop_0based = [i - 1 for i in drop_1based]
    to_keep = [i for i in range(df.shape[1]) if i not in to_drop_0based]
    return df.iloc[:, to_keep].copy()

def step2_reorder_supnam_lpo(df: pd.DataFrame) -> pd.DataFrame:
    cols = list(df.columns)
    if "SUPNAM" in cols:
        cols.remove("SUPNAM")
        cols = ["SUPNAM"] + cols
    if "LPO" in cols:
        cols.remove("LPO")
        cols = cols + ["LPO"]
    return df[cols]

def _group_blocks(df: pd.DataFrame):
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

def _arrange_blocks_no_adjacent_same_store(blocks):
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
    for c in ["STR NAME", "LPO"]:
        if c not in df.columns:
            raise ValueError(f"Missing required column '{c}' after Step 2. Found: {list(df.columns)}")
    blocks = _group_blocks(df)
    arranged = _arrange_blocks_no_adjacent_same_store(blocks)
    out_df = pd.concat([b["df"] for b in arranged], axis=0).reset_index(drop=True)
    return out_df, arranged

def to_excel_download(df: pd.DataFrame) -> bytes:
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Orders")
    buf.seek(0)
    return buf.read()

with st.expander("Instructions", expanded=True):
    st.markdown("""
1. Upload the **Carrefour raw** Excel file.
2. The app will:
   - **Step 1:** delete columns **1,2,4,5,6,7,9,11**  
   - **Step 2:** reorder columns so `SUPNAM` is first and `LPO` is last  
   - **Step 3:** group by (`STR NAME`, `LPO`) and arrange blocks to avoid **adjacent same-store** groups where possible.
3. Preview results and download the transformed Excel.
    """)

uploaded = st.file_uploader("Upload Carrefour raw.xlsx", type=["xlsx"])

keep_intermediate = st.checkbox("Also produce intermediate (after Step 1 & 2)", value=True)

if uploaded is not None:
    try:
        raw_df = pd.read_excel(uploaded)
        st.subheader("Raw file (head)")
        st.dataframe(raw_df.head(20), use_container_width=True)

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

            # Step 3
            df3, arranged_blocks = step3_group_and_arrange(df2)

            # Block order preview
            block_order_df = pd.DataFrame(
                [{"#": i+1, "STR NAME": b["store"], "LPO": b["lpo"], "first_row_index": b["first_idx"]}
                 for i, b in enumerate(arranged_blocks)]
            )
            st.subheader("Order of (STR NAME, LPO) blocks")
            st.dataframe(block_order_df, use_container_width=True, height=300)

            # --- Schedule-based day filtering ---
            st.markdown("---")
            st.subheader("Day-based filtering by shop schedule")


            # Try to load default schedule config
            schedule_df = None
            schedule_error = None
            try:
                schedule_df = load_shop_schedule(CONFIG_SCHEDULE_PATH)
            except Exception as e:
                schedule_error = str(e)

            if schedule_error:
                st.error(f"Could not load default shop schedule from '{CONFIG_SCHEDULE_PATH}': {schedule_error}")
            else:
                st.info(f"Using shop schedule from '{CONFIG_SCHEDULE_PATH}'. Columns: STR NAME, allowed_day (1–7).")


                # Day selector
                selected_day = st.selectbox(
                    "Select delivery day (1=Mon … 7=Sun)",
                    options=list(DAY_LABELS.keys()),
                    format_func=lambda x: f"{x} — {DAY_LABELS.get(x, '?')}"
                )

                # Prepare schedule mapping: STR NAME -> set of allowed days
                sched_map = {}
                for _, row in schedule_df.iterrows():
                    store = str(row["STR NAME"]).strip()
                    day = int(row["allowed_day"]) if not pd.isna(row["allowed_day"]) else None
                    if not store or day is None:
                        continue
                    sched_map.setdefault(store, set()).add(day)

                # Apply filtering to df3
                df3_work = df3.copy()
                df3_work["_store"] = df3_work["STR NAME"].astype(str).str.strip()

                def is_allowed(store: str) -> bool:
                    days = sched_map.get(store, None)
                    if not days:
                        return False
                    return selected_day in days

                df3_work["ALLOWED_DAY"] = df3_work["_store"].apply(is_allowed)

                allowed_df = df3_work[df3_work["ALLOWED_DAY"]].drop(columns=["_store"]).reset_index(drop=True)
                wrong_df = df3_work[~df3_work["ALLOWED_DAY"]].drop(columns=["_store"]).reset_index(drop=True)

                col1, col2 = st.columns(2)
                with col1:
                    st.markdown("**Allowed for selected day**")
                    st.write(f"Rows: {len(allowed_df)}")
                    st.dataframe(allowed_df.head(50), use_container_width=True)
                with col2:
                    st.markdown("**WRONG DAY (not allowed for selected day)**")
                    st.write(f"Rows: {len(wrong_df)}")
                    st.dataframe(wrong_df.head(50), use_container_width=True)

                st.markdown("Download by day:")
                c1, c2 = st.columns(2)
                with c1:
                    st.download_button(
                        label="Download ALLOWED (by day)",
                        data=to_excel_download(allowed_df),
                        file_name="Carrefour_transformed_ALLOWED.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    )
                with c2:
                    st.download_button(
                        label="Download WRONG DAY",
                        data=to_excel_download(wrong_df),
                        file_name="Carrefour_transformed_WRONG_DAY.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    )

            st.subheader("Final output (head)")
            st.dataframe(df3.head(50), use_container_width=True)

            # Downloads
            st.download_button(
                label="Download TRANSFORMED (Steps 1–3)",
                data=to_excel_download(df3),
                file_name="Carrefour_transformed.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

            if keep_intermediate:
                st.download_button(
                    label="Download INTERMEDIATE (after Step 1 & 2)",
                    data=to_excel_download(df2),
                    file_name="Carrefour_after_step1_2.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )

            st.success("Transformation complete. Note: inputs are not modified; only row ordering is changed to avoid ERP merge issues.")

    except Exception as e:
        st.error(f"Error: {e}")
else:
    st.info("Upload the raw Carrefour order file to begin.")
