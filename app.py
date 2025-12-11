import streamlit as st
import pandas as pd
import numpy as np
import io
import re
from datetime import date

# ============================================================
# Streamlit App: Tab-based Lookup Updater (Strict)
# - Lookup is Excel with sheets:
#   Generic, Family, Die, Package, Option, Packing
# - Exact input column names (no candidates)
# - Preserve "N/A" as string (not converted to NaN)
# - Validate columns per-level (rows can be valid for some levels and not others)
# - Map existing strings -> IDs
# - Create new sequential IDs per level
# - Append new rows to the relevant tab with RowFlag/RowDate/RunTag
# - Export updated lookup workbook + enriched data + rejected rows
# ============================================================

st.set_page_config(page_title="Hierarchy Lookup Builder (Tabs + Strict)", layout="wide")

# ----------------------------
# 0) Default config
# ----------------------------
DEFAULT_LOOKUP_PATH = "lookup_.xlsx"
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
def apply_normalized_columns_to_tab(tab_df: pd.DataFrame, level_key: str):
    if level_key not in {"Family", "Die"}:
        return tab_df

    info = LEVEL_TABS[level_key]
    i_col = info["id_col"]  # raw ID col

    if level_key == "Family":
        s_norm = "Family String_Normalized"
        i_norm = "Family ID_Normalized"

        # Build normalized string from the tab's component cols
        tab_df[s_norm] = (
            tab_df[COL_BV].map(safe_str) + "|" +
            tab_df[COL_IC].map(safe_str) + "|" +
            tab_df[COL_VCS].map(strip_after_at)
        ).str.strip("|")

    else:  # Die
        s_norm = "Die String_Normalized"
        i_norm = "Die ID_Normalized"

        tab_df[s_norm] = tab_df[COL_HFE].map(strip_after_at)

    # Map each normalized string -> least RAW ID
    norm_map = (
        tab_df[tab_df[s_norm].map(safe_str) != ""]
        .groupby(s_norm)[i_col]
        .apply(min_id)
        .to_dict()
    )

    tab_df[i_norm] = tab_df[s_norm].map(norm_map).fillna("")
    return tab_df

def is_blank_or_na(x):
    s = str(x).strip()
    if s == "":
        return True
    if s.upper() in {t.upper() for t in BAD_TOKENS}:
        return True
    return False


def level_valid_mask(df: pd.DataFrame, level_key: str):
    comps = LEVEL_TABS[level_key]["components"]
    mask = pd.Series(True, index=df.index)
    for c in comps:
        mask = mask & (~df[c].astype(str).map(is_blank_or_na))
    return mask


def build_level_issue_table(df: pd.DataFrame):
    issue = pd.DataFrame(index=df.index)
    for level_key in LEVEL_TABS.keys():
        issue[f"{level_key}_Valid"] = level_valid_mask(df, level_key)
    issue["Any_Level_Valid"] = issue[[c for c in issue.columns if c.endswith("_Valid")]].any(axis=1)
    return issue.reset_index().rename(columns={"index": "RowIndex"})

# ----------------------------
# 4) General helpers
# ----------------------------
def safe_str(x):
    if pd.isna(x):
        return ""
    return str(x).strip()


def strip_after_at(x):
    s = safe_str(x)
    if "@" in s:
        s = s.split("@", 1)[0]
    return s.strip()


def id_sort_key(v: str):
    v = safe_str(v)
    p, n, _w = extract_prefix_and_num(v)
    return (p or "", n if n is not None else 10 ** 12, v)


def min_id(series: pd.Series):
    vals = [safe_str(x) for x in series.tolist() if safe_str(x)]
    if not vals:
        return ""
    return sorted(vals, key=id_sort_key)[0]

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

    # Extra normalized cols only for Family/Die
    extra = []
    if level_key == "Family":
        extra = ["Family String_Normalized", "Family ID_Normalized"]
    elif level_key == "Die":
        extra = ["Die String_Normalized", "Die ID_Normalized"]

    # Ensure core + extra cols exist
    for c in comps + [s_col, i_col] + extra + META_COLS:
        if c not in df.columns:
            df[c] = ""

    # Normalize meta defaults for pre-existing rows
    df["RowFlag"] = df["RowFlag"].apply(safe_str)
    df["RowDate"] = df["RowDate"].apply(safe_str)
    df["RunTag"]  = df["RunTag"].apply(safe_str)
    df.loc[df["RowFlag"] == "", "RowFlag"] = "original"

    # Final column order
    ordered = comps + [s_col, i_col] + extra + META_COLS
    df = df[ordered].copy()
    return df


def build_strings_from_input(df: pd.DataFrame):
    out = df.copy()

    # --- normalized raw feature values
    out[f"{COL_VCS}_Normalized"] = out[COL_VCS].map(strip_after_at)
    out[f"{COL_HFE}_Normalized"] = out[COL_HFE].map(strip_after_at)

    # --- Generic
    out["Generic String"] = (
        out[COL_TRANSISTOR_TYPE].map(safe_str) + "|" +
        out[COL_CONFIGURATION].map(safe_str)
    ).str.strip("|")

    # --- Family RAW
    out["Family String"] = (
        out[COL_BV].map(safe_str) + "|" +
        out[COL_IC].map(safe_str) + "|" +
        out[COL_VCS].map(safe_str)
    ).str.strip("|")

    # --- Family NORMALIZED
    out["Family String_Normalized"] = (
        out[COL_BV].map(safe_str) + "|" +
        out[COL_IC].map(safe_str) + "|" +
        out[f"{COL_VCS}_Normalized"].map(safe_str)
    ).str.strip("|")

    # --- Die RAW
    out["Die String"] = out[COL_HFE].map(safe_str)

    # --- Die NORMALIZED
    out["Die String_Normalized"] = out[f"{COL_HFE}_Normalized"].map(safe_str)

    # --- Package
    out["Package String"] = (
        out[COL_PKG].map(safe_str) + "|" +
        out[COL_PIN].map(safe_str) + "|" +
        out[COL_PMAX].map(safe_str)
    ).str.strip("|")

    # --- Option
    out["Option String"] = (
        out[COL_TEMP].map(safe_str) + "|" +
        out[COL_ROHS].map(safe_str)
    ).str.strip("|")

    # --- Packing
    out["Packing String"] = (
        out[COL_PACK_TYPE].map(safe_str) + "|" +
        out[COL_PACK_QTY].map(normalize_pack_qty) + "|" +
        out[COL_REEL_DIA].map(safe_str)
    ).str.strip("|")

    return out

def assign_ids_for_level(enriched: pd.DataFrame, tab_df: pd.DataFrame,
                         level_key: str, run_tag: str, prefix_override: str,
                         valid_mask: pd.Series):
    info = LEVEL_TABS[level_key]
    s_col = info["string_col"]   # RAW string col name
    i_col = info["id_col"]       # RAW ID col name
    comps = info["components"]
    default_prefix = prefix_override or info["prefix_default"]

    # Ensure lookup tab schema (now keeps extra normalized cols for Family/Die)
    tab_df = ensure_tab_columns(tab_df, level_key)

    # Normalize existing lookup RAW
    tab_df[s_col] = tab_df[s_col].apply(safe_str)
    tab_df[i_col] = tab_df[i_col].apply(safe_str)
    for c in comps:
        if c in tab_df.columns:
            tab_df[c] = tab_df[c].apply(safe_str)

    existing = tab_df[tab_df[s_col] != ""].copy()
    mapping_raw = dict(zip(existing[s_col], existing[i_col]))

    # Infer RAW ID format
    prefix, next_num, width = infer_id_format(existing[i_col], default_prefix)

    # ---------------------------------------
    # PASS 1) RAW DISTINCT RELATIONS (VALID rows only)
    # ---------------------------------------
    rel_cols = comps + [s_col]
    rel_df = enriched.loc[valid_mask, rel_cols].copy()

    for c in comps:
        rel_df[c] = rel_df[c].apply(safe_str)
    rel_df[s_col] = rel_df[s_col].apply(safe_str)

    rel_df = rel_df[rel_df[s_col] != ""].copy()
    rel_df = rel_df.drop_duplicates(subset=rel_cols, keep="first").reset_index(drop=True)

    new_rel = rel_df[~rel_df[s_col].isin(mapping_raw.keys())].copy()

    new_rows = []
    if not new_rel.empty:
        new_ids = []
        for _ in range(len(new_rel)):
            new_ids.append(format_id(prefix, next_num, width))
            next_num += 1

        new_rel[i_col] = new_ids

        # Update RAW mapping
        for s, new_id in zip(new_rel[s_col].tolist(), new_rel[i_col].tolist()):
            mapping_raw[s] = new_id

        # Build new lookup rows with original comps
        for _, r in new_rel.iterrows():
            row = {c: "" for c in comps + [s_col, i_col] + META_COLS}
            for c in comps:
                row[c] = r[c]
            row[s_col] = r[s_col]
            row[i_col] = r[i_col]
            row["RowFlag"] = "added"
            row["RowDate"] = TODAY_STR
            row["RunTag"]  = run_tag or ""
            new_rows.append(row)

    if new_rows:
        tab_df = pd.concat([tab_df, pd.DataFrame(new_rows)], ignore_index=True)

    # ---------------------------------------
    # Map RAW IDs back to enriched
    # ---------------------------------------
    enriched[i_col] = ""
    valid_strings = enriched.loc[valid_mask, s_col].apply(safe_str)
    enriched.loc[valid_mask, i_col] = valid_strings.map(mapping_raw).fillna("")

    # ---------------------------------------
    # PASS 2) NORMALIZED (Family/Die only)
    # - add 2 new cols in tab + enriched
    # - normalized ID = least RAW ID per normalized string
    # ---------------------------------------
    if level_key in {"Family", "Die"}:
        tab_df = apply_normalized_columns_to_tab(tab_df, level_key)

        if level_key == "Family":
            s_norm = "Family String_Normalized"
            i_norm = "Family ID_Normalized"
        else:
            s_norm = "Die String_Normalized"
            i_norm = "Die ID_Normalized"

        # Build norm_map from UPDATED tab
        norm_map = (
            tab_df[tab_df[s_norm].map(safe_str) != ""]
            .groupby(s_norm)[i_col]
            .apply(min_id)
            .to_dict()
        )

        # Ensure enriched cols exist
        if s_norm not in enriched.columns:
            # should already exist from build_strings_from_input
            enriched[s_norm] = ""
        enriched[i_norm] = ""

        norm_series = enriched[s_norm].map(safe_str)
        enriched[s_norm] = norm_series
        enriched.loc[valid_mask, i_norm] = norm_series.loc[valid_mask].map(norm_map).fillna("")

    return enriched, tab_df, prefix, width, len(new_rows)



def reorder_enriched_cols(df: pd.DataFrame):
    preferred = []
    for base in ["Generic", "Family", "Die", "Package", "Option", "Packing"]:
        s = f"{base} String"
        sn = f"{base} String_Normalized"
        i = f"{base} ID"
        inn = f"{base} ID_Normalized"
        for c in [s, sn, i, inn]:
            if c in df.columns:
                preferred.append(c)

    rest = [c for c in df.columns if c not in preferred]
    return df[rest[:0] + preferred + rest]


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


def build_partid_codes(enriched_df: pd.DataFrame):
    if "PartID" not in enriched_df.columns:
        return None

    cols = ["PartID"]
    id_cols = [info["id_col"] for info in LEVEL_TABS.values()]
    cols.extend([c for c in id_cols if c in enriched_df.columns])

    partid_df = enriched_df[cols].copy()
    partid_df = partid_df.drop_duplicates().reset_index(drop=True)
    return partid_df

def load_lookup_from_file(path: str):
    with open(path, "rb") as f:
        content = f.read()
    xls = pd.ExcelFile(io.BytesIO(content))
    sheets = {}
    for key in LEVEL_TABS.keys():
        sheet_name = LEVEL_TABS[key]["sheet"]
        if sheet_name in xls.sheet_names:
            sheets[key] = pd.read_excel(
                xls,
                sheet_name=sheet_name,
                keep_default_na=False,
                na_filter=False,
            )
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
This app reads the **local lookup_.xlsx** bundled with the app and expects it to
have these sheets only: **Generic, Family, Die, Package, Option, Packing**.

Rules:
- Uses your input columns **as-is**.
- Validity is checked per level using only that level's columns.
- New strings get new sequential IDs per tab.
- New lookup rows are stamped with **RowFlag/RowDate/RunTag**.
"""
)

with st.sidebar:
    st.header("Lookup Source")
    st.caption(f"Using repository file: {DEFAULT_LOOKUP_PATH}")

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
if st.button("Load lookup from repository"):
    try:
        lookup_tabs = load_lookup_from_file(DEFAULT_LOOKUP_PATH)
        st.session_state["lookup_tabs"] = lookup_tabs
        st.success("Lookup tabs loaded.")
    except FileNotFoundError:
        st.error(f"Lookup file not found at: {DEFAULT_LOOKUP_PATH}")
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
        st.error("Load lookup from the repository file first.")
    elif raw_df is None:
        st.error("Upload your raw data first.")
    else:
        try:
            # Validate required columns exist
            missing = [c for c in REQUIRED_COLS if c not in raw_df.columns]
            if missing:
                raise KeyError("Missing required columns:\n- " + "\n- ".join(missing))

            # Build strings on all rows (no global rejection)
            enriched = build_strings_from_input(raw_df)

            # Build per-level validity table
            issues = build_level_issue_table(raw_df)
            st.session_state["issues_table"] = issues
            st.session_state["rejected_rows"] = pd.DataFrame()

            # We'll update tabs copy
            updated_tabs = {}

            st.subheader("ID assignment summary")

            # Assign level by level
            for level_key in LEVEL_TABS.keys():
                tab_df = lookup_tabs.get(level_key, pd.DataFrame())
                issues = st.session_state["issues_table"].set_index("RowIndex")
                valid_mask = issues[f"{level_key}_Valid"]
                enriched, updated_tab, prefix, width, new_count = assign_ids_for_level(
                    enriched=enriched,
                    tab_df=tab_df,
                    level_key=level_key,
                    run_tag=run_tag,
                    prefix_override=prefix_override.get(level_key),
                    valid_mask=valid_mask
                )
                updated_tabs[level_key] = updated_tab

                st.write(
                    f"**{level_key}** → prefix `{prefix}`, width `{width}`. "
                    f"New rows added: **{new_count}**."
                )

            # Store results
            enriched = reorder_enriched_cols(enriched)
            st.session_state["enriched_df"] = enriched
            st.session_state["updated_tabs"] = updated_tabs
            st.session_state["partid_codes"] = build_partid_codes(enriched)

            st.success("Mapping complete.")

            m1, m2 = st.columns(2)
            m1.metric("Raw rows", len(raw_df))
            issues = st.session_state["issues_table"]
            m2.metric("Rows valid for ANY level", int(issues["Any_Level_Valid"].sum()))

            st.subheader("Enriched data preview (all rows)")
            st.dataframe(enriched.head(80000), use_container_width=True)

            st.subheader("Validity by level (first 50 rows)")
            st.dataframe(issues.head(50), use_container_width=True)

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
partid_codes = st.session_state.get("partid_codes")

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

    if partid_codes is not None:
        st.download_button(
            "Download PartID codes (CSV)",
            data=partid_codes.to_csv(index=False).encode("utf-8-sig"),
            file_name="partid_codes.csv",
            mime="text/csv"
        )
    else:
        st.caption("Add a PartID column to your input to enable PartID-level codes export.")
else:
    st.info("Run mapping to enable downloads.")

st.caption(
    """
The app reads the repository copy of lookup_.xlsx and generates an updated
workbook for download. Commit the updated file back to your repo as needed.
"""
)
