import streamlit as st
import pandas as pd
import numpy as np
import io
import re
import requests
from datetime import date

# ============================================================
# Streamlit App: Tab-based Lookup Updater (Strict)
# - Lookup is Excel with sheets:
#   Generic, Family, Die, Package, Option, Packing
# - Exact input column names (no candidates)
# - Reject row if ANY required column is Blank or literal "N/A"
# - Preserve "N/A" as string (not converted to NaN)
# - Map existing strings -> IDs
# - Create new sequential IDs per level
# - Append new rows to the relevant tab with RowFlag/RowDate/RunTag
# - Export updated lookup workbook + enriched data + rejected rows
# ============================================================

st.set_page_config(page_title="Hierarchy Lookup Builder (Tabs + Strict)", layout="wide")

# ----------------------------
# 0) Default config
# ----------------------------
DEFAULT_LOOKUP_URL = "https://raw.githubusercontent.com/<YOUR_USER>/<YOUR_REPO>/main/lookup_.xlsx"
TODAY_STR = date.today().isoformat()

# ----------------------------
# 1) Exact input column names
# ----------------------------
COL_TRANSISTOR_TYPE = "Transistor Type"
COL_CONFIGURATION   = "Configuration"

COL_BV   = "Maximum Collector Emitter Breakdown Voltage"
COL_IC   = "Maximum Collector Current"
COL_VCS  = "Maximum Collector Emitter Saturation Voltage"

COL_HFE  = "Minimum DC Current Gain"

COL_PKG  = "Package_NormalizedPackageName"
COL_PIN  = "PinoutStr"
COL_PMAX = "Maximum Power Dissipation"

COL_TEMP = "ZTemperatureGrade"
COL_ROHS = "RohsStatus"

COL_PACK_TYPE = "Packaging_Type"
COL_PACK_QTY  = "PackagingQuantity"
COL_REEL_DIA  = "ReelDiameter"

REQUIRED_COLS = [
    COL_TRANSISTOR_TYPE, COL_CONFIGURATION,
    COL_BV, COL_IC, COL_VCS,
    COL_HFE,
    COL_PKG, COL_PIN, COL_PMAX,
    COL_TEMP, COL_ROHS,
    COL_PACK_TYPE, COL_PACK_QTY, COL_REEL_DIA
]

# ----------------------------
# 2) Lookup tab definitions
# ----------------------------
LEVEL_TABS = {
    "Generic": {
        "sheet": "Generic",
        "string_col": "Generic String",
        "id_col": "Generic ID",
        "prefix_default": "G",
        "components": [COL_TRANSISTOR_TYPE, COL_CONFIGURATION]
    },
    "Family": {
        "sheet": "Family",
        "string_col": "Family String",
        "id_col": "Family ID",
        "prefix_default": "F",
        "components": [COL_BV, COL_IC, COL_VCS]
    },
    "Die": {
        "sheet": "Die",
        "string_col": "Die String",
        "id_col": "Die ID",
        "prefix_default": "D",
        "components": [COL_HFE]
    },
    "Package": {
        "sheet": "Package",
        "string_col": "Package String",
        "id_col": "Package ID",
        "prefix_default": "P",
        "components": [COL_PKG, COL_PIN, COL_PMAX]
    },
    "Option": {
        "sheet": "Option",
        "string_col": "Option String",
        "id_col": "Option ID",
        "prefix_default": "O",
        "components": [COL_TEMP, COL_ROHS]
    },
    "Packing": {
        "sheet": "Packing",
        "string_col": "Packing String",
        "id_col": "Packing ID",
        "prefix_default": "R",
        "components": [COL_PACK_TYPE, COL_PACK_QTY, COL_REEL_DIA]
    },
}

META_COLS = ["RowFlag", "RowDate", "RunTag"]

# ----------------------------
# 3) Strict rejection settings
# ----------------------------
BAD_TOKENS = {"N/A"}  # literal token

def is_blank_or_na(x):
    s = str(x).strip()
    if s == "":
        return True
    if s.upper() in {t.upper() for t in BAD_TOKENS}:
        return True
    return False

def reject_rows(df: pd.DataFrame, required_cols):
    invalid = pd.Series(False, index=df.index)
    for c in required_cols:
        invalid = invalid | df[c].astype(str).map(is_blank_or_na)
    rejected = df[invalid].copy()
    accepted = df[~invalid].copy()
    return accepted, rejected

# ----------------------------
# 4) General helpers
# ----------------------------
def safe_str(x):
    if pd.isna(x):
        return ""
    return str(x).strip()

def normalize_pack_qty(x):
    if pd.isna(x):
        return ""
    s = str(x).strip()
    try:
        f = float(s)
        if abs(f - int(round(f))) < 1e-9:
            return str(int(round(f)))
        return str(f).rstrip("0").rstrip(".")
    except:
        return s

def extract_prefix_and_num(id_value: str):
    s = safe_str(id_value)
    m = re.match(r"^([A-Za-z]+)\s*0*(\d+)$", s)
    if not m:
        return None, None, None
    prefix = m.group(1)
    digits_part = re.search(r"(\d+)$", s).group(1)
    width = len(digits_part)
    num = int(m.group(2))
    return prefix, num, width

def infer_id_format(existing_ids: pd.Series, default_prefix: str):
    ids = existing_ids.dropna().astype(str).map(str.strip)
    parsed = []
    for v in ids:
        p, n, w = extract_prefix_and_num(v)
        if p and n is not None:
            parsed.append((p, n, w))

    if not parsed:
        return default_prefix, 1, 2

    pref = [x for x in parsed if x[0].upper() == default_prefix.upper()]
    use = pref if pref else parsed

    prefix = use[0][0]
    max_num = max(x[1] for x in use)
    widths = [x[2] for x in use if x[0].upper() == prefix.upper()]
    width = max(widths) if widths else max(x[2] for x in use)

    return prefix, max_num + 1, width

def format_id(prefix: str, num: int, width: int):
    return f"{prefix}{str(num).zfill(width)}"

def ensure_tab_columns(df: pd.DataFrame, level_key: str):
    info = LEVEL_TABS[level_key]
    s_col = info["string_col"]
    i_col = info["id_col"]
    comps = info["components"]

    # Ensure core cols
    for c in comps + [s_col, i_col] + META_COLS:
        if c not in df.columns:
            df[c] = ""

    # Normalize meta defaults for pre-existing rows
    df["RowFlag"] = df["RowFlag"].apply(safe_str)
    df["RowDate"] = df["RowDate"].apply(safe_str)
    df["RunTag"]  = df["RunTag"].apply(safe_str)

    df.loc[df["RowFlag"] == "", "RowFlag"] = "original"

    # Final column order
    df = df[comps + [s_col, i_col] + META_COLS].copy()
    return df

def build_strings_from_input(df: pd.DataFrame):
    out = df.copy()

    out["Generic String"] = (
        out[COL_TRANSISTOR_TYPE].map(safe_str) + "|" +
        out[COL_CONFIGURATION].map(safe_str)
    ).str.strip("|")

    out["Family String"] = (
        out[COL_BV].map(safe_str) + "|" +
        out[COL_IC].map(safe_str) + "|" +
        out[COL_VCS].map(safe_str)
    ).str.strip("|")

    out["Die String"] = out[COL_HFE].map(safe_str)

    out["Package String"] = (
        out[COL_PKG].map(safe_str) + "|" +
        out[COL_PIN].map(safe_str) + "|" +
        out[COL_PMAX].map(safe_str)
    ).str.strip("|")

    out["Option String"] = (
        out[COL_TEMP].map(safe_str) + "|" +
        out[COL_ROHS].map(safe_str)
    ).str.strip("|")

    out["Packing String"] = (
        out[COL_PACK_TYPE].map(safe_str) + "|" +
        out[COL_PACK_QTY].map(normalize_pack_qty) + "|" +
        out[COL_REEL_DIA].map(safe_str)
    ).str.strip("|")

    return out

def assign_ids_for_level(enriched: pd.DataFrame, tab_df: pd.DataFrame, level_key: str, run_tag: str, prefix_override: str):
    info = LEVEL_TABS[level_key]
    s_col = info["string_col"]
    i_col = info["id_col"]
    comps = info["components"]
    default_prefix = prefix_override or info["prefix_default"]

    tab_df = ensure_tab_columns(tab_df, level_key)

    # Build mapping
    tab_df[s_col] = tab_df[s_col].apply(safe_str)
    tab_df[i_col] = tab_df[i_col].apply(safe_str)
    existing = tab_df[tab_df[s_col] != ""].copy()

    mapping = dict(zip(existing[s_col], existing[i_col]))
    prefix, next_num, width = infer_id_format(existing[i_col], default_prefix)

    # Assign for input rows
    new_rows = []
    out_ids = []

    for idx, row in enriched.iterrows():
        s = safe_str(row.get(s_col, ""))
        if s == "":
            out_ids.append("")
            continue

        if s in mapping:
            out_ids.append(mapping[s])
            continue

        # new ID
        new_id = format_id(prefix, next_num, width)
        next_num += 1
        mapping[s] = new_id
        out_ids.append(new_id)

        # build new lookup row (with original component cols)
        new_lookup_row = {c: "" for c in comps + [s_col, i_col] + META_COLS}
        for c in comps:
            new_lookup_row[c] = safe_str(row.get(c, ""))

        new_lookup_row[s_col] = s
        new_lookup_row[i_col] = new_id
        new_lookup_row["RowFlag"] = "added"
        new_lookup_row["RowDate"] = TODAY_STR
        new_lookup_row["RunTag"]  = run_tag or ""

        new_rows.append(new_lookup_row)

    enriched[i_col] = out_ids

    # Append deduped new rows to tab
    if new_rows:
        add_df = pd.DataFrame(new_rows)
        tab_df = pd.concat([tab_df, add_df], ignore_index=True)

        # Dedup by string
        tab_df[s_col] = tab_df[s_col].apply(safe_str)
        tab_df = tab_df.drop_duplicates(subset=[s_col], keep="first").reset_index(drop=True)

    return enriched, tab_df, prefix, width, len(new_rows)

def to_excel_bytes_with_tabs(enriched_df, rejected_df, tabs_dict):
    bio = io.BytesIO()
    with pd.ExcelWriter(bio, engine="xlsxwriter") as w:
        enriched_df.to_excel(w, index=False, sheet_name="Enriched_Data")
        rejected_df.to_excel(w, index=False, sheet_name="Rejected_Rows")

        for level_key, tdf in tabs_dict.items():
            sheet = LEVEL_TABS[level_key]["sheet"]
            tdf.to_excel(w, index=False, sheet_name=sheet[:31])

    bio.seek(0)
    return bio.getvalue()

def load_lookup_from_github(url: str):
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    content = r.content
    # Read all sheets
    xls = pd.ExcelFile(io.BytesIO(content))
    sheets = {}
    for key in LEVEL_TABS.keys():
        sheet_name = LEVEL_TABS[key]["sheet"]
        if sheet_name in xls.sheet_names:
            sheets[key] = pd.read_excel(xls, sheet_name=sheet_name,
                                        keep_default_na=False, na_filter=False)
        else:
            # Create empty sheet if missing
            sheets[key] = pd.DataFrame()
    return sheets

# ----------------------------
# 5) UI
# ----------------------------
st.title("Hierarchy Lookup Builder (Tabs + Strict Rows)")

st.markdown(
    """
This app expects your **GitHub lookup Excel** to have these sheets only:
**Generic, Family, Die, Package, Option, Packing**.

Rules:
- Uses your input columns **as-is**.
- If any required field is **Blank** or **N/A**, the row is **rejected**.
- New strings get new sequential IDs per tab.
- New lookup rows are stamped with **RowFlag/RowDate/RunTag**.
"""
)

with st.sidebar:
    st.header("Lookup Source")
    lookup_url = st.text_input("GitHub RAW URL (lookup_.xlsx)", value=DEFAULT_LOOKUP_URL)

    st.header("Run Tag")
    run_tag = st.text_input("Optional run label", value="")

    st.header("Prefix overrides (optional)")
    prefix_override = {}
    for level_key in LEVEL_TABS.keys():
        prefix_override[level_key] = st.text_input(
            f"{level_key} prefix",
            value=LEVEL_TABS[level_key]["prefix_default"]
        )

    st.divider()
    st.caption("Required input columns:")
    st.code("\n".join(REQUIRED_COLS))

# Load lookup button
lookup_tabs = None
if st.button("Load lookup from GitHub"):
    try:
        lookup_tabs = load_lookup_from_github(lookup_url)
        st.session_state["lookup_tabs"] = lookup_tabs
        st.success("Lookup tabs loaded.")
    except Exception as e:
        st.error(f"Failed to load lookup: {e}")

if "lookup_tabs" in st.session_state:
    lookup_tabs = st.session_state["lookup_tabs"]
    st.subheader("Lookup preview (first 10 rows per tab)")
    cols = st.columns(3)
    i = 0
    for level_key, df_tab in lookup_tabs.items():
        with cols[i % 3]:
            st.write(f"**{level_key}**")
            st.dataframe(df_tab.head(10), use_container_width=True)
        i += 1

st.divider()

# Upload raw data
data_file = st.file_uploader("Upload your raw data (Excel/CSV)", type=["xlsx", "xls", "csv"])

raw_df = None
if data_file:
    try:
        if data_file.name.lower().endswith(".csv"):
            raw_df = pd.read_csv(
                data_file,
                keep_default_na=False,
                na_filter=False
            )
        else:
            raw_df = pd.read_excel(
                data_file,
                sheet_name=0,
                keep_default_na=False,
                na_filter=False
            )
        st.session_state["raw_df"] = raw_df
        st.success(f"Loaded data: {raw_df.shape[0]} rows × {raw_df.shape[1]} cols")
        st.dataframe(raw_df.head(50), use_container_width=True)
    except Exception as e:
        st.error(f"Failed to read data: {e}")
elif "raw_df" in st.session_state:
    raw_df = st.session_state["raw_df"]

st.divider()

# Run button
run = st.button("Run mapping + update lookup", type="primary")

if run:
    if lookup_tabs is None:
        st.error("Load lookup from GitHub first.")
    elif raw_df is None:
        st.error("Upload your raw data first.")
    else:
        try:
            # Validate required columns exist
            missing = [c for c in REQUIRED_COLS if c not in raw_df.columns]
            if missing:
                raise KeyError("Missing required columns:\n- " + "\n- ".join(missing))

            # Reject invalid rows
            accepted, rejected = reject_rows(raw_df, REQUIRED_COLS)
            st.session_state["rejected_rows"] = rejected

            # Build strings on accepted only
            enriched = build_strings_from_input(accepted)

            # We'll update tabs copy
            updated_tabs = {}

            st.subheader("ID assignment summary")

            # Assign level by level
            for level_key in LEVEL_TABS.keys():
                tab_df = lookup_tabs.get(level_key, pd.DataFrame())
                enriched, updated_tab, prefix, width, new_count = assign_ids_for_level(
                    enriched=enriched,
                    tab_df=tab_df,
                    level_key=level_key,
                    run_tag=run_tag,
                    prefix_override=prefix_override.get(level_key)
                )
                updated_tabs[level_key] = updated_tab

                st.write(
                    f"**{level_key}** → prefix `{prefix}`, width `{width}`. "
                    f"New rows added: **{new_count}**."
                )

            # Store results
            st.session_state["enriched_df"] = enriched
            st.session_state["updated_tabs"] = updated_tabs

            st.success("Mapping complete.")

            m1, m2, m3 = st.columns(3)
            m1.metric("Raw rows", len(raw_df))
            m2.metric("Accepted rows", len(enriched))
            m3.metric("Rejected rows", len(rejected))

            st.subheader("Enriched data preview (accepted rows only)")
            st.dataframe(enriched.head(100), use_container_width=True)

            st.subheader("Rejected rows preview")
            if rejected.empty:
                st.info("No rejected rows.")
            else:
                st.dataframe(rejected.head(100), use_container_width=True)

            st.subheader("Updated lookup tab previews (tail 20)")
            cols = st.columns(3)
            i = 0
            for level_key, tdf in updated_tabs.items():
                with cols[i % 3]:
                    st.write(f"**{level_key}**")
                    st.dataframe(tdf.tail(20), use_container_width=True)
                i += 1

        except KeyError as e:
            st.error(str(e))
        except Exception as e:
            st.error(f"Unexpected error: {e}")

st.divider()

# Downloads
enriched = st.session_state.get("enriched_df")
updated_tabs = st.session_state.get("updated_tabs")
rejected = st.session_state.get("rejected_rows", pd.DataFrame())

if enriched is not None and updated_tabs is not None:
    # Excel bundle containing updated lookup tabs + enriched + rejected
    bundle_bytes = to_excel_bytes_with_tabs(enriched, rejected, updated_tabs)

    c1, c2, c3 = st.columns(3)
    with c1:
        st.download_button(
            "Download UPDATED lookup + data (Excel)",
            data=bundle_bytes,
            file_name="hierarchy_tabs_outputs.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    with c2:
        st.download_button(
            "Download Enriched Data (CSV)",
            data=enriched.to_csv(index=False).encode("utf-8-sig"),
            file_name="enriched_data.csv",
            mime="text/csv"
        )
    with c3:
        st.download_button(
            "Download Rejected Rows (CSV)",
            data=rejected.to_csv(index=False).encode("utf-8-sig"),
            file_name="rejected_rows.csv",
            mime="text/csv"
        )

    # Optional: export each tab as CSV too
    with st.expander("Download each lookup tab as CSV"):
        for level_key, tdf in updated_tabs.items():
            st.download_button(
                f"{level_key} CSV",
                data=tdf.to_csv(index=False).encode("utf-8-sig"),
                file_name=f"{level_key.lower()}_lookup.csv",
                mime="text/csv"
            )
else:
    st.info("Run mapping to enable downloads.")

st.caption(
    """
You will manually commit the updated lookup Excel back to GitHub.
This app reads from GitHub RAW and generates an updated workbook for download.
"""
)
