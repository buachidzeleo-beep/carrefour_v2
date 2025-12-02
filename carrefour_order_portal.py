import io
import pandas as pd
import streamlit as st
from datetime import datetime
from typing import List, Dict, Any, Tuple

st.set_page_config(page_title="Carrefour Order Transformer", layout="wide")

# -------------------------------------------------------
# CONFIG
# -------------------------------------------------------
DROP_COLS_1BASED = [1, 2, 4, 5, 6, 7, 9, 11]
EXPECTED_AFTER = ["SUPNAM", "STR NAME", "BARCODE", "DESC", "QTYORD", "CP", "LPO"]
CONFIG_SCHEDULE_PATH = "config/carrefour_shop_schedule.xlsx"

DAY_LABELS = {
    1: "Monday", 2: "Tuesday", 3: "Wednesday",
    4: "Thursday", 5: "Friday", 6: "Saturday", 7: "Sunday",
}

# -------------------------------------------------------
# CORE HELPERS
# -------------------------------------------------------

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


def _group_blocks(df: pd.DataFrame) -> List[Dict[str, Any]]:
    df = df.reset_index(drop=False).rename(columns={"index": "_orig_idx"})
    seen = set()
    order_keys = []
    for _, r in df.iterrows():
        key = (r["STR NAME"], r["LPO"])
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
            "df": blk.sort_values("_orig_idx").drop(columns=["_orig_idx"]).reset_index(drop=True),
        })

    return blocks


def _arrange_blocks(blocks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
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


def step3_group_and_arrange(df: pd.DataFrame):
    blocks = _group_blocks(df)
    arranged = _arrange_blocks(blocks)
    out_df = pd.concat([b["df"] for b in arranged], axis=0).reset_index(drop=True)
    return out_df


def load_schedule(path: str) -> pd.DataFrame:
    df = pd.read_excel(path)
    df.columns = [c.strip() for c in df.columns]

    if not {"STR", "allowed_day"}.issubset(df.columns):
        raise ValueError("Schedule requires STR + allowed_day columns.")

    df["allowed_day"] = pd.to_numeric(df["allowed_day"], errors="coerce")
    df = df[df["allowed_day"].between(1, 7)]
    df["STR_KEY"] = df["STR"].astype(str).str.strip().str.upper()
    return df


def filter_raw_by_str_day(raw_df: pd.DataFrame, schedule_df: pd.DataFrame, selected_day: int):
    df = raw_df.copy()

    # identify STR code source
    if "STR" in df.columns:
        df["STR_TMP"] = df["STR"].astype(str)
    else:
        df["STR_TMP"] = df.iloc[:, 1].astype(str)

    df["STR_KEY"] = df["STR_TMP"].str.strip().str.upper()

    sched_map: Dict[str, set] = {}
    for _, r in schedule_df.iterrows():
        sched_map.setdefault(r["STR_KEY"], set()).add(int(r["allowed_day"]))

    df["ALLOWED"] = df["STR_KEY"].apply(lambda x: selected_day in sched_map.get(x, set()))

    raw_allowed = df[df["ALLOWED"]].drop(columns=["STR_TMP", "STR_KEY", "ALLOWED"])
    raw_wrong = df[~df["ALLOWED"]].drop(columns=["STR_TMP", "STR_KEY", "ALLOWED"])
    return raw_allowed.reset_index(drop=True), raw_wrong.reset_index(drop=True)


def transform_steps(raw_subset: pd.DataFrame):
    if raw_subset.empty:
        return pd.DataFrame()

    df1 = step1_delete_columns(raw_subset, DROP_COLS_1BASED)
    df2 = step2_reorder_supnam_lpo(df1)

    missing = [c for c in EXPECTED_AFTER if c not in df2.columns]
    if missing:
        raise ValueError(f"Missing columns after step1/2: {missing}")

    df3 = step3_group_and_arrange(df2)
    return df3


def to_excel(df: pd.DataFrame, filename: str):
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as w:
        df.to_excel(w, index=False, sheet_name="Orders")
    buf.seek(0)
    return buf.getvalue(), filename


# -------------------------------------------------------
# UI START
# -------------------------------------------------------

st.header("Carrefour Order Transformation")

selected_day = st.selectbox(
    "Select day (1–7)",
    options=list(DAY_LABELS.keys()),
    format_func=lambda x: f"{x} — {DAY_LABELS[x]}"
)

disable_filter = st.checkbox("Disable STR-day filtering (process full file)", value=False)

uploaded = st.file_uploader("Upload Carrefour raw file (.xlsx)", type=["xlsx"])

if uploaded:
    try:
        raw_df = pd.read_excel(uploaded)

        # load schedule
        schedule_df = load_schedule(CONFIG_SCHEDULE_PATH)

        # -----------------------------------------
        # FILTER OR BYPASS
        # -----------------------------------------
        if disable_filter:
            raw_allowed = raw_df.copy()
            raw_wrong = pd.DataFrame()
        else:
            raw_allowed, raw_wrong = filter_raw_by_str_day(raw_df, schedule_df, selected_day)

        # -----------------------------------------
        # APPLY STEPS 1–3
        # -----------------------------------------
        df_allowed = transform_steps(raw_allowed)
        df_wrong = transform_steps(raw_wrong)

        # -----------------------------------------
        # STORES WITHOUT ORDERS
        # -----------------------------------------
        all_str_in_raw = set(raw_df.iloc[:, 1].astype(str).str.upper())
        all_str_in_schedule = set(schedule_df["STR_KEY"])
        missing_stores = sorted(list(all_str_in_schedule - all_str_in_raw))

        missing_df = pd.DataFrame({"STR (no order today)": missing_stores})

        # -----------------------------------------
        # DISPLAY TWO FINAL TABLES
        # -----------------------------------------
        st.subheader("დაშვებული შეკვეთები")
        st.dataframe(df_allowed, use_container_width=True)

        if not disable_filter:
            st.subheader("არასწორი დღე")
            st.dataframe(df_wrong, use_container_width=True)

        st.subheader("მაღაზიები რომლებმაც არ გააკეთეს შეკვეთა დღეს")
        st.dataframe(missing_df, use_container_width=True)

        # -----------------------------------------
        # DOWNLOAD BUTTONS
        # -----------------------------------------
        today = datetime.today().strftime("%Y-%m-%d")

        if disable_filter:
            file_bytes, fname = to_excel(
                df_allowed,
                f"Carrefour - სრული ფაილი - {today}.xlsx"
            )
            st.download_button("Download full file", data=file_bytes, file_name=fname)
        else:
            file_bytes, fname = to_excel(
                df_allowed,
                f"Carrefour - დაშვებული შეკვეთები - {today}.xlsx"
            )
            st.download_button("Download allowed", data=file_bytes, file_name=fname)

            file_bytes2, fname2 = to_excel(
                df_wrong,
                f"Carrefour - არასწორი დღე - {today}.xlsx"
            )
            st.download_button("Download wrong-day", data=file_bytes2, file_name=fname2)

    except Exception as e:
        st.error(f"Error: {e}")
