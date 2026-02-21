import streamlit as st
import pandas as pd
import datetime
import json
from core import (
    execute_query,
    get_db_connection,
    stack_lead,
    get_table_schema,
    get_pipeline_counts,
    get_leads_by_stage,
    update_stage,
    get_all_tags_with_counts,
    get_leads_by_tag,
    rename_tag,
    remove_tag_from_all,
    save_saved_search,
    list_saved_searches,
    get_saved_search,
    delete_saved_search,
    get_dashboard_stats,
    add_uploaded_list,
    list_uploaded_lists,
    update_uploaded_list_status,
    delete_uploaded_lists,
    count_properties,
    delete_properties,
    UPLOAD_STATUSES,
)


def _norm(s):
    """Normalize header for matching: lower, strip, collapse spaces/underscores."""
    if s is None or pd.isna(s):
        return ""
    return " ".join(str(s).strip().lower().replace("_", " ").replace("-", " ").split())


# Default CSV header patterns for auto-matching (order = priority)
DEFAULT_IMPORT_MAP = {
    # Property Location (Essential)
    "property_address": ["property address", "address", "street address", "prop address", "property_address", "situs address", "street", "property ad"],
    "property_city": ["property city", "city", "prop city", "property_city", "property city name"],
    "property_state": ["property state", "state", "prop state", "property_state", "st", "property sta"],
    "property_zip": ["property zip", "zip", "zip code", "property_zip", "property zip code", "zipcode", "prop zip"],
    "property_county": ["property county", "county", "property_county", "prop county"],
    
    # Owner Names (Essential)
    "first_name": ["first name", "firstname", "first_name", "owner first", "owner first name", "owner 1 first name", "fname"],
    "last_name": ["last name", "lastname", "last_name", "owner last", "owner last name", "owner 1 last name", "lname"],
    "owner_2_first_name": ["owner 2 first name", "owner2 first name", "owner 2 first", "second owner first"],
    "owner_2_last_name": ["owner 2 last name", "owner2 last name", "owner 2 last", "second owner last"],
    "owner_3_first_name": ["owner 3 first name", "owner3 first name", "owner 3 first", "third owner first"],
    "owner_3_last_name": ["owner 3 last name", "owner3 last name", "owner 3 last", "third owner last"],
    "owner_4_first_name": ["owner 4 first name", "owner4 first name", "owner 4 first", "fourth owner first"],
    "owner_4_last_name": ["owner 4 last name", "owner4 last name", "owner 4 last", "fourth owner last"],
    
    # Mailing Address
    "mailing_address": ["mailing address", "mailing_address", "mailing ad", "mail address", "mail ad", "owner address", "owner mailing address"],
    "mailing_city": ["mailing city", "mailing_city", "mail city", "mailing city name", "owner mailing city"],
    "mailing_state": ["mailing state", "mailing_state", "mail state", "mailing sta", "mail state", "owner mailing state"],
    "mailing_zip": ["mailing zip", "mailing_zip", "mail zip", "mailing zip code", "mail zip code", "owner mailing zip"],
    
    # Phones
    "phone_1": ["phone 1", "phone1", "phone_1", "phone", "cell", "mobile", "telephone", "primary phone", "owner phone"],
    "phone_2": ["phone 2", "phone2", "phone_2", "secondary phone", "alt phone"],
    "phone_3": ["phone 3", "phone3", "phone_3"],
    "phone_4": ["phone 4", "phone4", "phone_4"],
    
    # Property Details
    "apn": ["apn", "parcel", "parcel id", "parcel number", "property a", "property id"],
    "property_type": ["property type", "property_type", "type", "prop type"],
    "property_use": ["property use", "property_use", "use", "prop use"],
    "land_use": ["land use", "land_use", "land use code"],
    "subdivision": ["subdivision", "sub", "subdivision name"],
    "legal_description": ["legal description", "legal_description", "legal desc", "legal"],
    
    # Property Size & Structure
    "living_sqft": ["living square feet", "living sqft", "living_sqft", "living sq ft", "sqft", "square feet", "living area"],
    "lot_acres": ["lot acres", "lot_acres", "acres", "lot size acres"],
    "lot_sqft": ["lot square feet", "lot sqft", "lot_sqft", "lot sq ft", "lot size"],
    "year_built": ["year built", "year_built", "built", "year", "construction year"],
    "stories": ["stories", "# of stories", "number of stories", "stories count", "floors"],
    "units_count": ["units count", "units_count", "units", "number of units", "# of units"],
    
    # Interior Features
    "beds": ["beds", "bedrooms", "bed", "br"],
    "baths": ["baths", "bathrooms", "bath", "ba"],
    "fireplaces": ["fireplaces", "# of fireplaces", "number of fireplaces", "fireplace count"],
    "ac_type": ["air conditioning type", "ac type", "air conditioning", "ac", "cooling type"],
    "heating_type": ["heating type", "heating", "heat type", "heating system"],
    
    # Garage/Carport
    "garage_type": ["garage type", "garage_type", "garage"],
    "garage_sqft": ["garage square feet", "garage sqft", "garage_sqft", "garage sq ft"],
    "carport": ["carport", "has carport"],
    "carport_area": ["carport area", "carport_area", "carport sqft"],
    
    # Owner Info
    "ownership_length_months": ["ownership length months", "ownership_length_months", "ownership length", "months owned"],
    "owner_type": ["owner type", "owner_type", "owner type code"],
    "owner_occupied": ["owner occupied", "owner_occupied", "owner occ", "occupied"],
    "vacant": ["vacant", "vacant?", "is vacant", "vacancy"],
    
    # Financial
    "occupancy": ["occupancy", "occupancy status", "occ", "occupied"],
    "est_value": ["est value", "estimated value", "value", "market value", "avm"],
    "last_sale_price": ["last sale price", "last sale", "sale price", "sales price", "sold price"],
}


def _match_column(csv_columns, field_key, used=None):
    """Return the first CSV column that matches any pattern for field_key, or None. Skip columns in used."""
    used = used or set()
    patterns = DEFAULT_IMPORT_MAP.get(field_key, [])
    normalized_csv = {c: _norm(c) for c in csv_columns}
    for p in patterns:
        for col, n in normalized_csv.items():
            if col in used:
                continue
            if p in n or n in p or n == p:
                return col
    return None


def _default_indices(csv_columns):
    """Return dict of field_key -> selectbox index. Each column used at most once for phone_1..4."""
    csv_list = list(csv_columns)
    used = set()
    out = {}
    for key in DEFAULT_IMPORT_MAP:
        matched = _match_column(csv_columns, key, used)
        if matched and matched in csv_list:
            out[key] = 1 + csv_list.index(matched)
            used.add(matched)
        else:
            out[key] = 0
    return out


# --------------------------------------------------
# PAGE CONFIG & GLOBAL STYLE
# --------------------------------------------------
st.set_page_config(layout="wide", page_title="RE Engine Pro", initial_sidebar_state="expanded")

CUSTOM_CSS = """
<style>
    /* Main container */
    .main .block-container { padding-top: 1.5rem; padding-bottom: 2rem; max-width: 100%; }
    /* Headers */
    h1 { font-size: 1.85rem !important; margin-bottom: 0.5rem !important; }
    h2 { font-size: 1.35rem !important; margin-top: 1rem !important; border-bottom: 1px solid var(--border-color, #eee); padding-bottom: 0.35rem !important; }
    h3 { font-size: 1.1rem !important; margin-top: 0.75rem !important; }
    /* Metrics and cards */
    [data-testid="stMetricValue"] { font-size: 1.5rem !important; }
    /* Buttons */
    .stButton > button { border-radius: 6px !important; font-weight: 500 !important; }
    .stButton > button[kind="primary"] { background-color: #0d6efd !important; }
    /* Expanders */
    .streamlit-expanderHeader { font-weight: 600 !important; }
    /* Tabs */
    .stTabs [data-baseweb="tab-list"] { gap: 0.25rem; margin-bottom: 0.75rem; }
    .stTabs [data-baseweb="tab"] { border-radius: 6px; padding: 0.5rem 1rem; }
    /* Sidebar */
    [data-testid="stSidebar"] { background: linear-gradient(180deg, #f8f9fa 0%, #fff 100%); }
    [data-testid="stSidebar"] .stExpander { border: 1px solid #eee; border-radius: 8px; margin-bottom: 0.5rem; }
    /* Spacing */
    hr { margin: 1rem 0 !important; }
    .stCaption { font-size: 0.9rem !important; }
</style>
"""
st.markdown(CUSTOM_CSS, unsafe_allow_html=True)

# --------------------------------------------------
# TOP BAR: App title + market filter
# --------------------------------------------------
_db_ok = True
try:
    _schema = get_table_schema()
    _cols = [r["column_name"] for r in _schema] if _schema else []
    STATE_COL = "state" if "state" in _cols else "property_state"
    _all_states_set = set()
    for col in ["state", "property_state"]:
        if col not in _cols:
            continue
        try:
            rows = execute_query(
                f"SELECT DISTINCT TRIM({col}) AS v FROM properties WHERE {col} IS NOT NULL AND TRIM({col}) != '' ORDER BY v",
                fetch=True,
            )
            if rows:
                for r in rows:
                    if r.get("v"):
                        _all_states_set.add(r["v"].strip())
        except Exception:
            pass
    all_states = sorted(_all_states_set)
except Exception as _e:
    _db_ok = False
    _schema = []
    _cols = []
    STATE_COL = "state"
    _all_states_set = set()
    all_states = []

top_col1, top_col2, top_col3 = st.columns([1, 2, 1])
with top_col1:
    st.markdown("### 🏠 RE Engine Pro")
with top_col2:
    st.markdown("<div style='text-align: center;'></div>", unsafe_allow_html=True)
with top_col3:
    st.markdown("**Market**")
    selected_state = st.selectbox(
        "Market",
        ["All States"] + all_states,
        key="top_market",
        label_visibility="collapsed",
    )
st.caption("Search, import, pipeline, and tags — all in one place.")
st.divider()

if not _db_ok:
    st.error(
        "**Cannot connect to the database.** On Streamlit Cloud, add your PostgreSQL credentials in **Manage app → Settings → Secrets**. "
        "Use either a `postgres` section (host, port, dbname, user, password) or flat keys: db_host, db_port, db_name, db_user, db_password. "
        "Ensure your database allows connections from the internet (Streamlit Cloud IPs)."
    )
    st.stop()

# --------------------------------------------------
# TABS
# --------------------------------------------------
tab_dash, tab1, tab2, tab_files, tab4 = st.tabs(
    ["📈 Dashboard", "🔍 Lead Engine", "📥 Bulk Import", "📁 My Files", "🏷 Tag Manager"]
)

# ==================================================
# TAB: DASHBOARD
# ==================================================
with tab_dash:
    st.header("📈 Dashboard")
    st.caption("Overview of your leads by market and pipeline stage.")
    try:
        stats = get_dashboard_stats()
        total = stats["total"]
        by_state = stats["by_state"]
        by_stage = stats["by_stage"]

        m1, m2, m3 = st.columns(3)
        with m1:
            st.metric("Total leads", f"{total:,}")
        with m2:
            st.metric("States", str(len(by_state)) if by_state else "0")
        with m3:
            st.metric("Pipeline stages", str(len(by_stage)) if by_stage else "0")

        st.divider()
        c1, c2 = st.columns(2)
        with c1:
            st.subheader("Leads by state")
            if by_state:
                state_df = pd.DataFrame(by_state)
                st.bar_chart(state_df.set_index("state")["cnt"])
            else:
                st.info("No state data yet. Import lists in **Bulk Import**.")
        with c2:
            st.subheader("Leads by pipeline stage")
            if by_stage:
                stage_df = pd.DataFrame(by_stage)
                st.bar_chart(stage_df.set_index("stage")["cnt"])
            else:
                st.info("No stages yet. Use **Lead Engine → Pipeline** or batch **Update** to set stages.")

        st.divider()
        st.subheader("Top states")
        if by_state:
            top_states = pd.DataFrame(by_state).head(5)
            for _, r in top_states.iterrows():
                st.caption(f"**{r['state']}**: {r['cnt']:,} leads")
        else:
            st.caption("No data yet.")
    except Exception as e:
        st.error(str(e))
        st.exception(e)

# ==================================================
# TAB 1: LEAD ENGINE (Search + Pipeline)
# ==================================================
with tab1:
    lead_sub_search, lead_sub_pipeline = st.tabs(["🔍 Search", "📊 Pipeline"])

    with lead_sub_search:
        st.header("🔎 Search")
        st.caption("Filter leads by property, owner, and financials. Save and load filter presets below.")

        # ---------- Saved Search ----------
        st.markdown("**Saved searches** — Save current filters with a name · **Load** runs the search · **Delete** removes the preset.")
        saved_list = list_saved_searches()
        saved_options = {f"{r['name']} (id:{r['id']})": r["id"] for r in saved_list}
        ss_col1, ss_col2, ss_col3, ss_col4, ss_col5 = st.columns([2, 1, 2, 1, 1])
        with ss_col1:
            saved_choice = st.selectbox("Choose saved search", ["— Select one —"] + list(saved_options.keys()), key="saved_search_choice")
        with ss_col2:
            load_clicked = st.button("Load", key="saved_load_btn", help="Run this search and show results")
        with ss_col3:
            save_name = st.text_input("Save current as", placeholder="e.g. OH absentee", key="saved_search_name")
        with ss_col4:
            save_clicked = st.button("Save", key="saved_save_btn", help="Save current filter settings with the name above")
        with ss_col5:
            delete_clicked = st.button("Delete", key="saved_delete_btn", help="Remove the selected saved search")
        if delete_clicked and saved_choice and saved_choice != "— Select one —":
            sid = saved_options.get(saved_choice)
            if sid:
                try:
                    delete_saved_search(sid)
                    st.success("Saved search deleted.")
                    st.rerun()
                except Exception as e:
                    st.error(f"Delete failed: {e}")
            else:
                st.warning("Select a saved search to delete.")
        if load_clicked and saved_choice and saved_choice != "— Select one —":
            try:
                sid = saved_options.get(saved_choice)
                if sid:
                    rec = get_saved_search(sid)
                    if rec:
                        fd = json.loads(rec["filters_json"])
                        # Build query from fd
                        show_only_multi = fd.get("show_only_multi", False)
                        min_appearances = fd.get("min_appearances", 2)
                        if show_only_multi:
                            query = """
                            SELECT p.*, addr.appearance_count
                            FROM properties p
                            INNER JOIN (
                                SELECT LOWER(TRIM(street_address)) as normalized_addr, COUNT(*) as appearance_count
                                FROM properties
                                WHERE street_address IS NOT NULL AND TRIM(street_address) != ''
                                GROUP BY LOWER(TRIM(street_address))
                                HAVING COUNT(*) >= %s
                            ) addr ON LOWER(TRIM(p.street_address)) = addr.normalized_addr
                            WHERE 1=1
                            """
                            params = [int(min_appearances)]
                        else:
                            query = "SELECT * FROM properties WHERE 1=1"
                            params = []
                        sel_state = fd.get("selected_state", "All States")
                        if sel_state != "All States":
                            prefix = "p." if show_only_multi else ""
                            if "state" in _cols and "property_state" in _cols:
                                query += f" AND ({prefix}state = %s OR {prefix}property_state = %s)"
                                params.extend([sel_state, sel_state])
                            else:
                                query += f" AND {prefix}{STATE_COL} = %s"
                                params.append(sel_state)
                        if fd.get("prop_types"):
                            query += " AND property_type = ANY(%s)"
                            params.append(fd["prop_types"])
                        if fd.get("bed_value"):
                            query += " AND beds >= %s"
                            params.append(fd["bed_value"])
                        if fd.get("bath_value"):
                            query += " AND baths >= %s"
                            params.append(fd["bath_value"])
                        if fd.get("occupancy_list"):
                            query += " AND occupancy_status = ANY(%s)"
                            params.append(fd["occupancy_list"])
                        if fd.get("apn_search"):
                            query += " AND apn ILIKE %s"
                            params.append(f"%{fd['apn_search']}%")
                        if fd.get("owner_name_contains"):
                            query += " AND owner_name ILIKE %s"
                            params.append(f"%{fd['owner_name_contains']}%")
                        if fd.get("owner_types"):
                            query += " AND owner_type = ANY(%s)"
                            params.append(fd["owner_types"])
                        if fd.get("is_absentee"):
                            query += " AND is_absentee = TRUE"
                        if fd.get("years_owned_min"):
                            query += " AND years_owned >= %s"
                            params.append(fd["years_owned_min"])
                        if fd.get("tax_year_max") and fd["tax_year_max"] < datetime.datetime.now().year:
                            query += " AND tax_delinquent_year <= %s"
                            params.append(fd["tax_year_max"])
                        if fd.get("est_value_min"):
                            query += " AND est_value >= %s"
                            params.append(fd["est_value_min"])
                        if fd.get("est_value_max"):
                            query += " AND est_value <= %s"
                            params.append(fd["est_value_max"])
                        if fd.get("est_equity_min"):
                            query += " AND est_equity_amt >= %s"
                            params.append(fd["est_equity_min"])
                        if fd.get("est_equity_max"):
                            query += " AND est_equity_amt <= %s"
                            params.append(fd["est_equity_max"])
                        if fd.get("est_equity_pct_min"):
                            query += " AND est_equity_pct >= %s"
                            params.append(fd["est_equity_pct_min"])
                        if fd.get("est_equity_pct_max") is not None and fd["est_equity_pct_max"] < 100:
                            query += " AND est_equity_pct <= %s"
                            params.append(fd["est_equity_pct_max"])
                        if fd.get("assessed_min"):
                            query += " AND assessed_total >= %s"
                            params.append(fd["assessed_min"])
                        if fd.get("assessed_max"):
                            query += " AND assessed_total <= %s"
                            params.append(fd["assessed_max"])
                        if fd.get("last_sale_min"):
                            query += " AND last_sale_price >= %s"
                            params.append(fd["last_sale_min"])
                        if fd.get("last_sale_max"):
                            query += " AND last_sale_price <= %s"
                            params.append(fd["last_sale_max"])
                        if fd.get("filter_by_sale_date") and fd.get("last_sale_date"):
                            query += " AND last_sale_date <= %s"
                            params.append(fd["last_sale_date"] if isinstance(fd["last_sale_date"], str) else fd["last_sale_date"])
                        if fd.get("private_loan"):
                            query += " AND has_private_loan = TRUE"
                        if fd.get("cash_buyer"):
                            query += " AND is_cash_buyer = TRUE"
                        if show_only_multi:
                            query += " ORDER BY addr.appearance_count DESC, street_address"
                        else:
                            query += " ORDER BY street_address"
                        conn = get_db_connection()
                        df = pd.read_sql(query, conn, params=params)
                        conn.close()
                        st.session_state["search_results"] = df
                        st.session_state["search_params"] = {"show_only_multi": show_only_multi, "min_appearances": min_appearances}
                        st.session_state["last_query"] = query
                        st.session_state["last_params"] = params
                        st.session_state["loaded_search_name"] = rec["name"]
                        st.session_state["loaded_search_criteria"] = fd
                        st.success(f"Loaded «{rec['name']}» — {len(df)} leads.")
                        st.rerun()
            except Exception as e:
                st.error(f"Load failed: {e}")
                st.exception(e)
        # Show what was loaded (so user knows what the current results represent)
        if st.session_state.get("loaded_search_name") and "search_results" in st.session_state:
            fd = st.session_state.get("loaded_search_criteria") or {}
            parts = []
            if fd.get("selected_state") and fd["selected_state"] != "All States":
                parts.append(f"State: {fd['selected_state']}")
            if fd.get("bed_value"):
                parts.append(f"Beds: {fd['bed_value']}+")
            if fd.get("bath_value"):
                parts.append(f"Baths: {fd['bath_value']}+")
            if fd.get("prop_types"):
                parts.append("Types: " + ", ".join(fd["prop_types"][:3]))
            if fd.get("show_only_multi"):
                parts.append(f"Multi-address ≥{fd.get('min_appearances', 2)}")
            if parts:
                st.info("**Loaded search:** «" + st.session_state["loaded_search_name"] + "» — " + " | ".join(parts))
        left, right = st.columns([4, 1])

        with left:
            # ============ FILTERS ============
            with st.expander("🏠 Property filters", expanded=True):
                st.markdown("**Property types**")
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    type_single_family = st.checkbox("Single-Family Homes", key="prop_single")
                with col2:
                    type_condo = st.checkbox("Condo/Co-Ownerships", key="prop_condo")
                with col3:
                    type_multi_2_4 = st.checkbox("Multi-Family (2-4)", key="prop_multi2")
                with col4:
                    type_multi_5plus = st.checkbox("Multi-Family (5+)", key="prop_multi5")
                prop_types = []
                if type_single_family:
                    prop_types.append("Single-Family Homes")
                if type_condo:
                    prop_types.append("Condo/Co-Ownerships")
                if type_multi_2_4:
                    prop_types.append("Multi-Family (2-4)")
                if type_multi_5plus:
                    prop_types.append("Multi-Family (5+)")

                st.divider()
                st.markdown("##### Bedrooms")
                f_beds = st.radio("Bedrooms", ["Any","1+","2+","3+","4+","5+"], horizontal=True, key="beds_radio")
                bed_value = 0 if f_beds == "Any" else int(f_beds.replace("+",""))

                st.divider()
                st.markdown("##### Bathrooms")
                f_baths = st.radio("Bathrooms", ["Any","1+","2+","3+","4+","5+"], horizontal=True, key="baths_radio")
                bath_value = 0 if f_baths == "Any" else int(f_baths.replace("+",""))

                st.divider()
                st.markdown("##### Occupancy Status")
                col1, col2 = st.columns(2)
                with col1:
                    occ_occupied = st.checkbox("Occupied", key="occ_occupied")
                with col2:
                    occ_vacant = st.checkbox("Vacant", key="occ_vacant")
                occupancy_list = []
                if occ_occupied:
                    occupancy_list.append("Occupied")
                if occ_vacant:
                    occupancy_list.append("Vacant")

                st.divider()
                st.markdown("##### APN Search")
                apn_search = st.text_input("APN Number", placeholder="Enter APN...", key="apn_search")

                with st.expander("👤 Owner Filters", expanded=False):
                    st.markdown("##### Owner Name Search")
                    owner_name_contains = st.text_input("Owner Name Contains", placeholder="Enter owner name...", key="owner_name_search")
    
                    st.divider()
                    st.markdown("##### Owner Type")
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        owner_individual = st.checkbox("Individual", key="owner_indiv")
                    with col2:
                        owner_business = st.checkbox("Business", key="owner_biz")
                    with col3:
                        owner_bank = st.checkbox("Bank or Trust", key="owner_bank")
                    owner_types = []
                    if owner_individual:
                        owner_types.append("Individual")
                    if owner_business:
                        owner_types.append("Business")
                    if owner_bank:
                        owner_types.append("Bank or Trust")
    
                    st.divider()
                    st.markdown("##### Absentee Owner")
                    is_absentee = st.checkbox("Absentee Owner Only", key="is_absentee")
    
                    st.divider()
                    st.markdown("##### Years Owned (Min)")
                    years_owned_min = st.number_input("Years Owned", min_value=0, value=0, step=1, key="years_owned")
    
                    st.divider()
                    st.markdown("##### Tax Delinquent Year")
                    tax_year_max = st.number_input(
                    "Max Tax Year",
                    min_value=2000,
                    max_value=datetime.datetime.now().year,
                    value=datetime.datetime.now().year,
                    step=1,
                    key="tax_year"
                    )
    
                with st.expander("💰 Financial Filters", expanded=False):
                    st.markdown("##### Estimated Value")
                    col1, col2 = st.columns(2)
                    with col1:
                        est_value_min = st.number_input("$ Min", min_value=0, value=0, step=10000, key="est_val_min")
                    with col2:
                        est_value_max = st.number_input("$ Max", min_value=0, value=0, step=10000, key="est_val_max")
    
                    st.divider()
                    st.markdown("##### Estimated Equity")
                    col1, col2 = st.columns(2)
                    with col1:
                        est_equity_min = st.number_input("$ Min", min_value=0, value=0, step=10000, key="est_eq_min")
                    with col2:
                        est_equity_max = st.number_input("$ Max", min_value=0, value=0, step=10000, key="est_eq_max")
    
                    st.divider()
                    st.markdown("##### Estimated Equity %")
                    col1, col2 = st.columns(2)
                    with col1:
                        est_equity_pct_min = st.number_input("Min %", min_value=0, max_value=100, value=0, step=5, key="est_eq_pct_min")
                    with col2:
                        est_equity_pct_max = st.number_input("Max %", min_value=0, max_value=100, value=100, step=5, key="est_eq_pct_max")
    
                    st.divider()
                    st.markdown("##### Assessed Total Value")
                    col1, col2 = st.columns(2)
                    with col1:
                        assessed_min = st.number_input("$ Min", min_value=0, value=0, step=10000, key="assessed_min")
                    with col2:
                        assessed_max = st.number_input("$ Max", min_value=0, value=0, step=10000, key="assessed_max")
    
                    st.divider()
                    st.markdown("##### Last Sale Price")
                    col1, col2 = st.columns(2)
                    with col1:
                        last_sale_min = st.number_input("$ Min", min_value=0, value=0, step=10000, key="sale_min")
                    with col2:
                        last_sale_max = st.number_input("$ Max", min_value=0, value=0, step=10000, key="sale_max")
    
                    st.divider()
                    st.markdown("##### Last Sale Date")
                    filter_by_sale_date = st.checkbox("Filter by last sale date (before date)", value=False, key="filter_sale_date")
                    last_sale_date = st.date_input("Before Date", value=datetime.datetime.now(), key="sale_date", disabled=not filter_by_sale_date)
    
                    st.divider()
                    st.markdown("##### Loan Type")
                    private_loan = st.checkbox("Private Loan Only", key="private_loan")
                    cash_buyer = st.checkbox("Cash Buyer Only", key="cash_buyer")
    
                with st.expander("📊 Address Appearance Filter", expanded=False):
                    st.markdown("##### Find addresses that appear multiple times")
                    col1, col2 = st.columns(2)
                    with col1:
                        min_appearances = st.number_input("Minimum appearances", min_value=1, value=2, step=1, key="min_appear")
                    with col2:
                        show_only_multi = st.checkbox("Show only multi-appearance addresses", value=False, key="show_multi")
    
                # ---------- Save current search ----------
                if save_clicked and save_name and save_name.strip():
                    filters_dict = {
                    "selected_state": selected_state,
                    "prop_types": prop_types,
                    "bed_value": bed_value,
                    "bath_value": bath_value,
                    "occupancy_list": occupancy_list,
                    "apn_search": apn_search or "",
                    "owner_name_contains": owner_name_contains or "",
                    "owner_types": owner_types,
                    "is_absentee": is_absentee,
                    "years_owned_min": years_owned_min,
                    "tax_year_max": tax_year_max,
                    "est_value_min": est_value_min,
                    "est_value_max": est_value_max,
                    "est_equity_min": est_equity_min,
                    "est_equity_max": est_equity_max,
                    "est_equity_pct_min": est_equity_pct_min,
                    "est_equity_pct_max": est_equity_pct_max,
                    "assessed_min": assessed_min,
                    "assessed_max": assessed_max,
                    "last_sale_min": last_sale_min,
                    "last_sale_max": last_sale_max,
                    "filter_by_sale_date": filter_by_sale_date,
                    "last_sale_date": last_sale_date.isoformat() if last_sale_date else None,
                    "private_loan": private_loan,
                    "cash_buyer": cash_buyer,
                    "show_only_multi": show_only_multi,
                    "min_appearances": min_appearances,
                    }
                    try:
                        save_saved_search(save_name.strip(), json.dumps(filters_dict))
                        st.success(f"Saved search «{save_name.strip()}».")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Save failed: {e}")
    
                # ============ RUN SEARCH BUTTON ============
                st.divider()
                if st.button("Run search", use_container_width=True, type="primary"):
                    st.session_state.pop("loaded_search_name", None)
                    st.session_state.pop("loaded_search_criteria", None)
                    # Build query
                    if show_only_multi:
                        query = """
                    SELECT p.*, addr.appearance_count
                    FROM properties p
                    INNER JOIN (
                        SELECT LOWER(TRIM(street_address)) as normalized_addr, COUNT(*) as appearance_count
                        FROM properties
                        WHERE street_address IS NOT NULL AND TRIM(street_address) != ''
                        GROUP BY LOWER(TRIM(street_address))
                        HAVING COUNT(*) >= %s
                    ) addr ON LOWER(TRIM(p.street_address)) = addr.normalized_addr
                    WHERE 1=1
                    """
                        params = [int(min_appearances)]  # ensure integer for HAVING
                    else:
                        query = "SELECT * FROM properties WHERE 1=1"
                        params = []

                    # State filter (match selected state in any state column so OH/NJ etc. from either column appear)
                    if selected_state != "All States":
                        prefix = "p." if show_only_multi else ""
                        if "state" in _cols and "property_state" in _cols:
                            query += f" AND ({prefix}state = %s OR {prefix}property_state = %s)"
                            params.append(selected_state)
                            params.append(selected_state)
                        else:
                            query += f" AND {prefix}{STATE_COL} = %s"
                            params.append(selected_state)
    
                    if prop_types:
                        query += " AND property_type = ANY(%s)"
                        params.append(prop_types)

                    if bed_value > 0:
                        query += " AND beds >= %s"
                        params.append(bed_value)

                    if bath_value > 0:
                        query += " AND baths >= %s"
                        params.append(bath_value)

                    if occupancy_list:
                        query += " AND occupancy_status = ANY(%s)"
                        params.append(occupancy_list)

                    if apn_search:
                        query += " AND apn ILIKE %s"
                        params.append(f"%{apn_search}%")

                    if owner_name_contains:
                        query += " AND owner_name ILIKE %s"
                        params.append(f"%{owner_name_contains}%")

                    if owner_types:
                        query += " AND owner_type = ANY(%s)"
                        params.append(owner_types)

                    if is_absentee:
                        query += " AND is_absentee = TRUE"

                    if years_owned_min > 0:
                        query += " AND years_owned >= %s"
                        params.append(years_owned_min)

                    if tax_year_max < datetime.datetime.now().year:
                        query += " AND tax_delinquent_year <= %s"
                        params.append(tax_year_max)

                    if est_value_min > 0:
                        query += " AND est_value >= %s"
                        params.append(est_value_min)
                    if est_value_max > 0:
                        query += " AND est_value <= %s"
                        params.append(est_value_max)

                    if est_equity_min > 0:
                        query += " AND est_equity_amt >= %s"
                        params.append(est_equity_min)
                    if est_equity_max > 0:
                        query += " AND est_equity_amt <= %s"
                        params.append(est_equity_max)

                    if est_equity_pct_min > 0:
                        query += " AND est_equity_pct >= %s"
                        params.append(est_equity_pct_min)
                    if est_equity_pct_max < 100:
                        query += " AND est_equity_pct <= %s"
                        params.append(est_equity_pct_max)

                    if assessed_min > 0:
                        query += " AND assessed_total >= %s"
                        params.append(assessed_min)
                    if assessed_max > 0:
                        query += " AND assessed_total <= %s"
                        params.append(assessed_max)

                    if last_sale_min > 0:
                        query += " AND last_sale_price >= %s"
                        params.append(last_sale_min)
                    if last_sale_max > 0:
                        query += " AND last_sale_price <= %s"
                        params.append(last_sale_max)

                    if filter_by_sale_date and last_sale_date:
                        query += " AND last_sale_date <= %s"
                        params.append(last_sale_date)

                    if private_loan:
                        query += " AND has_private_loan = TRUE"

                    if cash_buyer:
                        query += " AND is_cash_buyer = TRUE"

                    if show_only_multi:
                        query += " ORDER BY addr.appearance_count DESC, street_address"
                    else:
                        query += " ORDER BY street_address"
    
                    # Store for debugging
                    st.session_state['last_query'] = query
                    st.session_state['last_params'] = params
    
                    # Execute
                    try:
                        conn = get_db_connection()
                        df = pd.read_sql(query, conn, params=params)
                        conn.close()
                        st.session_state['search_results'] = df
                        st.session_state['search_params'] = {
                            'show_only_multi': show_only_multi,
                            'min_appearances': min_appearances
                        }
                        st.rerun()
                    except Exception as e:
                        st.error(f"Query error: {str(e)}")
                        st.exception(e)
    
                # ============ DISPLAY RESULTS WITH BATCH ACTIONS ============
                # If user changed multi-appearance settings, don't show stale results
                if 'search_results' in st.session_state and 'search_params' in st.session_state:
                    saved_multi = st.session_state['search_params'].get('show_only_multi')
                    saved_min = st.session_state['search_params'].get('min_appearances')
                    if saved_multi != show_only_multi or saved_min != min_appearances:
                        del st.session_state['search_results']
                        del st.session_state['search_params']
                        st.info("Multi-appearance settings changed. Click **Run search** to refresh (e.g. min appearances = {}).".format(min_appearances))
                if 'search_results' in st.session_state:
                    df = st.session_state['search_results']
                    show_only_multi = st.session_state['search_params']['show_only_multi']
                    min_appearances = st.session_state['search_params']['min_appearances']
    
                    if show_only_multi:
                        unique_addrs = df['street_address'].nunique() if 'street_address' in df.columns else 0
                        st.subheader(f"🏠 {len(df)} Properties Found ({unique_addrs} unique addresses with ≥{min_appearances} appearances)")
                        if len(df) > 0 and 'appearance_count' in df.columns:
                            st.bar_chart(df['appearance_count'].value_counts().sort_index())
                            st.caption(f"Addresses appearing {min_appearances}+ times across all lists.")
                    else:
                        st.subheader(f"🏠 {len(df)} Leads Found")
    
                    if len(df) > 0:
                        original_cols = len(df.columns)
                    # Column filter: hide empty columns
                    show_all_cols = st.checkbox("Show all columns (including empty)", value=False, key="show_all_cols")
                    
                    # Filter out columns that are mostly None/empty (including string "None")
                    if not show_all_cols:
                        # Always-essential columns (show even if empty)
                        always_show = {'id', 'street_address', 'city', 'owner_name'}
                        # Conditionally essential (show only if they have real data, not "None")
                        conditional_essential = {'state', 'property_state', 'phone_numbers', 'tags', 'stage', 
                                               'motivation_score', 'last_list_source', 'zip_code', 'apn', 
                                               'property_type', 'beds', 'baths'}
                        
                        def _has_real_data(series):
                            """Check if series has real data (not None/null/empty/"None"/"nan")."""
                            # First check for NaN/null
                            non_null = series.notna()
                            if not non_null.any():
                                return 0
                
                            if series.dtype == 'object':
                                # Convert to string, strip, check for real values
                                s_str = series.astype(str).str.strip().str.lower()
                                # Count rows that are not empty and not "none"/"nan"/"null"
                                empty_values = ['none', 'nan', '', 'null', '<na>', 'na', 'none']
                                # Also exclude rows where original was NaN (pandas shows as 'nan' string)
                                valid = (non_null & (~s_str.isin(empty_values))).sum()
                                return valid
                            else:
                                # Numeric - count non-null
                                return non_null.sum()
                        
                        cols_to_keep = []
                        for col in df.columns:
                            if col in always_show:
                                cols_to_keep.append(col)
                            elif col in conditional_essential:
                                # Only keep if >30% have real data (not mostly "None")
                                valid_count = _has_real_data(df[col])
                                if valid_count > len(df) * 0.3:  # At least 30% have real data
                                    cols_to_keep.append(col)
                            else:
                                # Other columns - only keep if >10% have real data
                                valid_count = _has_real_data(df[col])
                                if valid_count > len(df) * 0.1:
                                    cols_to_keep.append(col)
                        
                        # Preserve column order from original df
                        cols_to_keep = [c for c in df.columns if c in cols_to_keep]
                        df = df[cols_to_keep]
                        hidden_count = original_cols - len(df.columns)
                        if hidden_count > 0:
                            st.caption(f"📊 Showing {len(df.columns)} columns ({hidden_count} empty columns hidden)")
                    
                    # Batch action toolbar with Select All
                    col1, col2, col3, col4, col5, col6 = st.columns([1,1,1,1,1,3])
                    with col1:
                        if st.button("Select all", use_container_width=True, key="btn_select_all"):
                            st.session_state['select_all_leads'] = True
                            st.rerun()
                        if st.button("Clear all", use_container_width=True, key="btn_clear_all"):
                            st.session_state['select_all_leads'] = False
                            st.rerun()
                    with col2:
                        if st.button("Update", use_container_width=True):
                            st.session_state['batch_action'] = 'update'
                    with col3:
                        if st.button("Tag", use_container_width=True):
                            st.session_state['batch_action'] = 'tag'
                    with col4:
                        if st.button("Export", use_container_width=True):
                            st.session_state['batch_action'] = 'export'
                    with col5:
                        if st.button("Delete", use_container_width=True):
                            st.session_state['batch_action'] = 'delete'

                    # Display dataframe with selection
                    df_with_sel = df.copy()
                    if 'selected' not in df_with_sel.columns:
                        df_with_sel.insert(0, 'selected', False)
                    
                    # Apply Select All / Clear All from session state
                    if st.session_state.get('select_all_leads') is True:
                        df_with_sel['selected'] = True
                    elif st.session_state.get('select_all_leads') is False:
                        df_with_sel['selected'] = False
                        # Clear the flag after applying
                        st.session_state.pop('select_all_leads', None)
    
                    edited_df = st.data_editor(
                        df_with_sel,
                        use_container_width=True,
                        hide_index=True,
                        column_config={
                            "selected": st.column_config.CheckboxColumn("Select", default=False)
                        },
                        disabled=[c for c in df_with_sel.columns if c != 'selected'],
                        key="lead_editor"
                    )
    
                    # Handle batch actions
                    if 'batch_action' in st.session_state:
                        selected = edited_df[edited_df['selected'] == True]
                        if len(selected) == 0:
                            st.warning("No leads selected.")
                            del st.session_state['batch_action']
                            st.rerun()
                        else:
                            if st.session_state['batch_action'] == 'export':
                                csv = selected.drop(columns=['selected']).to_csv(index=False)
                                st.download_button(
                                    label="📥 Download CSV",
                                    data=csv,
                                    file_name=f"selected_leads_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                                    mime="text/csv"
                                )
                                del st.session_state['batch_action']
                                st.rerun()
    
                            elif st.session_state['batch_action'] == 'delete':
                                st.error(f"Are you sure you want to delete {len(selected)} leads? This cannot be undone.")
                                col1, col2 = st.columns(2)
                                with col1:
                                    if st.button("✅ Yes, Delete"):
                                        ids = tuple(selected['id'].tolist())
                                        if len(ids) == 1:
                                            ids = f"({ids[0]})"
                                        else:
                                            ids = str(ids)
                                        delete_query = f"DELETE FROM properties WHERE id IN {ids}"
                                        execute_query(delete_query)
                                        st.success(f"Deleted {len(selected)} leads.")
                                        del st.session_state['batch_action']
                                        del st.session_state['search_results']  # refresh
                                        st.rerun()
                                with col2:
                                    if st.button("❌ Cancel"):
                                        del st.session_state['batch_action']
                                        st.rerun()
    
                            elif st.session_state['batch_action'] == 'update':
                                st.subheader(f"Batch Update {len(selected)} Leads")
                                with st.form("batch_update_form"):
                                    new_motivation = st.number_input("Set Motivation Score (0 to skip)", min_value=0, max_value=10, value=0)
                                    new_stage = st.selectbox("Set Stage", ["", "New", "Contacted", "Negotiating", "Closed", "Lost"], index=0)
                                    new_notes = st.text_area("Add Notes (appended to existing notes)")
                                    col1, col2 = st.columns(2)
                                    with col1:
                                        submitted = st.form_submit_button("Update")
                                    with col2:
                                        cancelled = st.form_submit_button("Cancel")
    
                                    if submitted:
                                        ids = list(selected['id'])
                                        updates = []
                                        params = []
                                        if new_motivation > 0:
                                            updates.append("motivation_score = %s")
                                            params.append(new_motivation)
                                        if new_stage:
                                            updates.append("stage = %s")
                                            params.append(new_stage)
                                        if new_notes:
                                            # Append notes with newline
                                            updates.append("notes = CONCAT(COALESCE(notes, ''), %s, '\n')")
                                            params.append(new_notes)
                                        if updates:
                                            placeholders = ','.join(['%s'] * len(ids))
                                            query = f"UPDATE properties SET {', '.join(updates)} WHERE id IN ({placeholders})"
                                            params.extend(ids)
                                            execute_query(query, params)
                                            st.success(f"Updated {len(ids)} leads.")
                                            del st.session_state['batch_action']
                                            del st.session_state['search_results']
                                            st.rerun()
                                        else:
                                            st.warning("No changes specified.")
                                    if cancelled:
                                        del st.session_state['batch_action']
                                        st.rerun()
    
                            elif st.session_state['batch_action'] == 'tag':
                                st.subheader(f"Manage Tags for {len(selected)} Leads")
                                # Use full search results for tags (displayed table may have hidden the tags column)
                                full_df = st.session_state['search_results']
                                selected_ids = selected['id'].tolist()
                                if 'tags' in full_df.columns:
                                    selected_with_tags = full_df[full_df['id'].isin(selected_ids)][['id', 'tags']]
                                    id_to_tags = selected_with_tags.set_index('id')['tags']
                                else:
                                    id_to_tags = pd.Series(dtype=object)  # no tags column
                                # Show current tags in selection
                                all_tags = set()
                                for tags in id_to_tags.dropna():
                                    all_tags.update([t.strip() for t in str(tags).split(',') if t.strip()])
                                st.write("Current tags in selection:", ", ".join(sorted(all_tags)) if all_tags else "None")
    
                                with st.form("batch_tag_form"):
                                    action = st.radio("Action", ["Add Tags", "Remove Tags"], horizontal=True)
                                    tags_input = st.text_input("Tags (comma-separated)", placeholder="e.g., hot, foreclosure, followup")
                                    col1, col2 = st.columns(2)
                                    with col1:
                                        submitted = st.form_submit_button("Apply")
                                    with col2:
                                        cancelled = st.form_submit_button("Cancel")
    
                                    if submitted:
                                        if tags_input:
                                            tag_list = [t.strip() for t in tags_input.split(',') if t.strip()]
                                            ids = list(selected['id'])
                                            if action == "Add Tags":
                                                for lead_id in ids:
                                                    current = id_to_tags.get(lead_id) if lead_id in id_to_tags.index else None
                                                    current_tags = set([t.strip() for t in str(current).split(',') if t.strip()]) if pd.notna(current) and current else set()
                                                    current_tags.update(tag_list)
                                                    new_tags = ', '.join(sorted(current_tags))
                                                    execute_query("UPDATE properties SET tags = %s WHERE id = %s", (new_tags, lead_id))
                                                st.success(f"Added tags to {len(ids)} leads.")
                                            else:  # Remove Tags
                                                for lead_id in ids:
                                                    current = id_to_tags.get(lead_id) if lead_id in id_to_tags.index else None
                                                    if pd.notna(current) and current:
                                                        current_tags = set([t.strip() for t in str(current).split(',') if t.strip()])
                                                        current_tags = current_tags - set(tag_list)
                                                        new_tags = ', '.join(sorted(current_tags)) if current_tags else None
                                                        execute_query("UPDATE properties SET tags = %s WHERE id = %s", (new_tags, lead_id))
                                                st.success(f"Removed tags from {len(ids)} leads.")
                                            del st.session_state['batch_action']
                                            del st.session_state['search_results']
                                            st.rerun()
                                        else:
                                            st.warning("No tags entered.")
                                    if cancelled:
                                        del st.session_state['batch_action']
                                        st.rerun()
                    else:
                        st.info("No results to display.")
                if 'search_results' not in st.session_state:
                    st.info("Run a search to see leads.")

                with st.expander("Debug (last query)", expanded=False):
                    if 'last_query' in st.session_state:
                        st.write("**Last Query:**")
                        st.code(st.session_state['last_query'])
                        st.write("**Parameters:**", st.session_state['last_params'])
                        try:
                            total = pd.read_sql("SELECT COUNT(*) as cnt FROM properties", get_db_connection()).iloc[0,0]
                            st.write(f"**Total records in database:** {total}")
                        except Exception as e:
                            st.error(f"Count query failed: {e}")
    
        # Right panel - applied filters
        with right:
                st.subheader("Active filters")
                filters = []
                if selected_state != "All States":
                    filters.append(f"**State:** {selected_state}")
                if prop_types:
                    filters.append(f"**Types:** {', '.join(prop_types)}")
                if bed_value > 0:
                    filters.append(f"**Beds:** {bed_value}+")
                if bath_value > 0:
                    filters.append(f"**Baths:** {bath_value}+")
                if occupancy_list:
                    filters.append(f"**Occupancy:** {', '.join(occupancy_list)}")
                if apn_search:
                    filters.append(f"**APN:** {apn_search}")
                if owner_name_contains:
                    filters.append(f"**Owner:** {owner_name_contains}")
                if owner_types:
                    filters.append(f"**Owner Type:** {', '.join(owner_types)}")
                if is_absentee:
                    filters.append("**Absentee Only**")
                if years_owned_min > 0:
                    filters.append(f"**Years Owned ≥ {years_owned_min}**")
                if tax_year_max < datetime.datetime.now().year:
                    filters.append(f"**Tax Year ≤ {tax_year_max}**")
                if est_value_min > 0 or est_value_max > 0:
                    filters.append(f"**Est Value:** ${est_value_min:,} - ${est_value_max:,}")
                if est_equity_min > 0 or est_equity_max > 0:
                    filters.append(f"**Est Equity:** ${est_equity_min:,} - ${est_equity_max:,}")
                if est_equity_pct_min > 0 or est_equity_pct_max < 100:
                    filters.append(f"**Equity %:** {est_equity_pct_min}% - {est_equity_pct_max}%")
                if assessed_min > 0 or assessed_max > 0:
                    filters.append(f"**Assessed:** ${assessed_min:,} - ${assessed_max:,}")
                if last_sale_min > 0 or last_sale_max > 0:
                    filters.append(f"**Last Sale:** ${last_sale_min:,} - ${last_sale_max:,}")
                if filter_by_sale_date and last_sale_date:
                    filters.append(f"**Last Sale Before:** {last_sale_date}")
                if private_loan:
                    filters.append("**Private Loan**")
                if cash_buyer:
                    filters.append("**Cash Buyer**")
                if show_only_multi:
                    filters.append(f"**Multi-List Only (≥{min_appearances})**")
    
                if filters:
                    for f in filters:
                        st.markdown(f)
                else:
                    st.info("No filters applied.")

    with lead_sub_pipeline:
        st.header("📊 Pipeline")
        st.caption("View leads by stage and move them between stages.")
        if "stage" not in _cols:
            st.warning("Your `properties` table has no `stage` column. Add it with: `ALTER TABLE properties ADD COLUMN stage VARCHAR(50);`")
        else:
            try:
                counts = get_pipeline_counts()
                if counts:
                    st.subheader("Counts by stage")
                    cols = st.columns(len(counts))
                    for i, row in enumerate(counts):
                        with cols[i]:
                            st.metric(row["stage"], row["cnt"])
                    st.divider()
                    stage_filter = st.selectbox(
                        "Show leads in stage",
                        ["All"] + [c["stage"] for c in counts],
                        key="pipeline_stage_filter",
                    )
                    leads_raw = get_leads_by_stage(stage_filter)
                    if not leads_raw:
                        st.info("No leads in this stage.")
                    else:
                        df_pl = pd.DataFrame(leads_raw)
                        st.caption(f"Total: {len(df_pl)} leads")
                        move_col, _ = st.columns([1, 3])
                        with move_col:
                            new_stage = st.selectbox(
                                "Move selected to stage",
                                ["New", "Contacted", "Negotiating", "Closed", "Lost"],
                                key="pipeline_new_stage",
                            )
                            move_btn = st.button("Apply", key="pipeline_move_btn")
                        df_pl_with_sel = df_pl.copy()
                        if "selected" not in df_pl_with_sel.columns:
                            df_pl_with_sel.insert(0, "selected", False)
                        edited_pl = st.data_editor(
                            df_pl_with_sel,
                            use_container_width=True,
                            hide_index=True,
                            column_config={"selected": st.column_config.CheckboxColumn("Select", default=False)},
                            disabled=[c for c in df_pl_with_sel.columns if c != "selected"],
                            key="pipeline_editor",
                        )
                        if move_btn:
                            selected = edited_pl[edited_pl["selected"] == True]
                            if len(selected) == 0:
                                st.warning("Select at least one lead.")
                            else:
                                ids = list(selected["id"])
                                update_stage(ids, new_stage)
                                st.success(f"Moved {len(ids)} lead(s) to **{new_stage}**.")
                                st.rerun()
                else:
                    st.info("No pipeline data yet. Use Lead Engine Search batch update to set stages.")
            except Exception as e:
                st.error(str(e))
                st.exception(e)

# ==================================================
# TAB: BULK IMPORT
# ==================================================
with tab2:
    st.header("📥 Bulk import")
    st.caption("Upload CSV files, map columns to your fields, and import leads. Duplicate addresses are kept and can be filtered in Lead Engine.")
    source_name = st.text_input("List source name (optional)", placeholder="e.g. Foreclosure list", key="bulk_source")
    uploaded_files = st.file_uploader("Upload CSV files", type=["csv"], accept_multiple_files=True, key="bulk_uploader")

    if uploaded_files:
        st.success(f"{len(uploaded_files)} file(s) ready — open a tab below to map and import.")
        file_tabs = st.tabs([f"📄 {f.name}" for f in uploaded_files])

        for idx, (tab, uploaded_file) in enumerate(zip(file_tabs, uploaded_files)):
            with tab:
                st.subheader(f"Map & import: {uploaded_file.name}")
                try:
                    # Read CSV
                    try:
                        raw_df = pd.read_csv(uploaded_file, encoding='utf-8')
                    except:
                        raw_df = pd.read_csv(uploaded_file, encoding='latin-1')
                    raw_df.columns = [str(col).strip() for col in raw_df.columns]

                    with st.expander("📋 Preview", expanded=False):
                        st.dataframe(raw_df.head(10))
                        st.caption(f"Total rows: {len(raw_df)} | Columns: {list(raw_df.columns)}")

                    st.divider()
                    st.subheader("Column mapping")
                    st.caption("Columns are auto-matched by header name. Adjust any mapping below if needed.")
                    csv_cols = list(raw_df.columns)
                    cols = ["None"] + csv_cols
                    di = _default_indices(csv_cols)
                    def _idx(k):
                        return min(di.get(k, 0), len(cols) - 1)

                    # ---- Essential (exact order): Property, Owner, Mailing, Phones ----
                    st.markdown("**Property**")
                    r1a, r1b, r1c, r1d, r1e = st.columns(5)
                    with r1a:
                        m_prop_addr = st.selectbox("Property Address *", cols, index=_idx("property_address"), key=f"prop_addr_{idx}")
                    with r1b:
                        m_prop_city = st.selectbox("Property City *", cols, index=_idx("property_city"), key=f"prop_city_{idx}")
                    with r1c:
                        m_prop_state = st.selectbox("Property State *", cols, index=_idx("property_state"), key=f"prop_state_{idx}")
                    with r1d:
                        m_prop_zip = st.selectbox("Property Zip", cols, index=_idx("property_zip"), key=f"prop_zip_{idx}")
                    with r1e:
                        m_prop_county = st.selectbox("Property County", cols, index=_idx("property_county"), key=f"prop_county_{idx}")

                    st.markdown("**Owner**")
                    r2a, r2b = st.columns(2)
                    with r2a:
                        m_first_name = st.selectbox("Owner 1 First Name *", cols, index=_idx("first_name"), key=f"first_name_{idx}")
                    with r2b:
                        m_last_name = st.selectbox("Owner 1 Last Name *", cols, index=_idx("last_name"), key=f"last_name_{idx}")
                    with st.expander("👥 Additional Owners (Owner 2-4)", expanded=False):
                        r2c, r2d = st.columns(2)
                        with r2c:
                            m_owner2_first = st.selectbox("Owner 2 First Name", cols, index=_idx("owner_2_first_name"), key=f"owner2_first_{idx}")
                        with r2d:
                            m_owner2_last = st.selectbox("Owner 2 Last Name", cols, index=_idx("owner_2_last_name"), key=f"owner2_last_{idx}")
                        r2e, r2f = st.columns(2)
                        with r2e:
                            m_owner3_first = st.selectbox("Owner 3 First Name", cols, index=_idx("owner_3_first_name"), key=f"owner3_first_{idx}")
                        with r2f:
                            m_owner3_last = st.selectbox("Owner 3 Last Name", cols, index=_idx("owner_3_last_name"), key=f"owner3_last_{idx}")
                        r2g, r2h = st.columns(2)
                        with r2g:
                            m_owner4_first = st.selectbox("Owner 4 First Name", cols, index=_idx("owner_4_first_name"), key=f"owner4_first_{idx}")
                        with r2h:
                            m_owner4_last = st.selectbox("Owner 4 Last Name", cols, index=_idx("owner_4_last_name"), key=f"owner4_last_{idx}")

                    st.markdown("**Mailing**")
                    r3a, r3b, r3c, r3d = st.columns(4)
                    with r3a:
                        m_mail_addr = st.selectbox("Mailing Address", cols, index=_idx("mailing_address"), key=f"mail_addr_{idx}")
                    with r3b:
                        m_mail_city = st.selectbox("Mailing City", cols, index=_idx("mailing_city"), key=f"mail_city_{idx}")
                    with r3c:
                        m_mail_state = st.selectbox("Mailing State", cols, index=_idx("mailing_state"), key=f"mail_sta_{idx}")
                    with r3d:
                        m_mail_zip = st.selectbox("Mailing Zip", cols, index=_idx("mailing_zip"), key=f"mail_zip_{idx}")

                    st.markdown("**Phones**")
                    r4a, r4b, r4c, r4d = st.columns(4)
                    with r4a:
                        m_phone1 = st.selectbox("Phone 1", cols, index=_idx("phone_1"), key=f"ph1_{idx}")
                    with r4b:
                        m_phone2 = st.selectbox("Phone 2", cols, index=_idx("phone_2"), key=f"ph2_{idx}")
                    with r4c:
                        m_phone3 = st.selectbox("Phone 3", cols, index=_idx("phone_3"), key=f"ph3_{idx}")
                    with r4d:
                        m_phone4 = st.selectbox("Phone 4", cols, index=_idx("phone_4"), key=f"ph4_{idx}")

                    # ---- Optional (APN, Beds, Baths, Value, etc.) ----
                    with st.expander("📊 Optional Fields", expanded=False):
                        st.markdown("**Property Details**")
                        col1, col2, col3, col4 = st.columns(4)
                        with col1:
                            m_apn = st.selectbox("APN", cols, index=_idx("apn"), key=f"apn_{idx}")
                        with col2:
                            m_prop_type = st.selectbox("Property Type", cols, index=_idx("property_type"), key=f"prop_type_{idx}")
                        with col3:
                            m_prop_use = st.selectbox("Property Use", cols, index=_idx("property_use"), key=f"prop_use_{idx}")
                        with col4:
                            m_land_use = st.selectbox("Land Use", cols, index=_idx("land_use"), key=f"land_use_{idx}")
                        col1, col2 = st.columns(2)
                        with col1:
                            m_subdivision = st.selectbox("Subdivision", cols, index=_idx("subdivision"), key=f"subdivision_{idx}")
                        with col2:
                            m_legal_desc = st.selectbox("Legal Description", cols, index=_idx("legal_description"), key=f"legal_desc_{idx}")
                        
                        st.markdown("**Property Size & Structure**")
                        col1, col2, col3, col4 = st.columns(4)
                        with col1:
                            m_living_sqft = st.selectbox("Living Square Feet", cols, index=_idx("living_sqft"), key=f"living_sqft_{idx}")
                        with col2:
                            m_lot_acres = st.selectbox("Lot (Acres)", cols, index=_idx("lot_acres"), key=f"lot_acres_{idx}")
                        with col3:
                            m_lot_sqft = st.selectbox("Lot (Square Feet)", cols, index=_idx("lot_sqft"), key=f"lot_sqft_{idx}")
                        with col4:
                            m_year_built = st.selectbox("Year Built", cols, index=_idx("year_built"), key=f"year_built_{idx}")
                        col1, col2, col3 = st.columns(3)
                        with col1:
                            m_beds = st.selectbox("Bedrooms", cols, index=_idx("beds"), key=f"beds_{idx}")
                        with col2:
                            m_baths = st.selectbox("Bathrooms", cols, index=_idx("baths"), key=f"baths_{idx}")
                        with col3:
                            m_stories = st.selectbox("# of Stories", cols, index=_idx("stories"), key=f"stories_{idx}")
                        col1, col2 = st.columns(2)
                        with col1:
                            m_units_count = st.selectbox("Units Count", cols, index=_idx("units_count"), key=f"units_count_{idx}")
                        with col2:
                            m_fireplaces = st.selectbox("# of Fireplaces", cols, index=_idx("fireplaces"), key=f"fireplaces_{idx}")
                        
                        st.markdown("**Garage & Carport**")
                        col1, col2, col3, col4 = st.columns(4)
                        with col1:
                            m_garage_type = st.selectbox("Garage Type", cols, index=_idx("garage_type"), key=f"garage_type_{idx}")
                        with col2:
                            m_garage_sqft = st.selectbox("Garage Square Feet", cols, index=_idx("garage_sqft"), key=f"garage_sqft_{idx}")
                        with col3:
                            m_carport = st.selectbox("Carport", cols, index=_idx("carport"), key=f"carport_{idx}")
                        with col4:
                            m_carport_area = st.selectbox("Carport Area", cols, index=_idx("carport_area"), key=f"carport_area_{idx}")
                        
                        st.markdown("**HVAC**")
                        col1, col2 = st.columns(2)
                        with col1:
                            m_ac_type = st.selectbox("Air Conditioning Type", cols, index=_idx("ac_type"), key=f"ac_type_{idx}")
                        with col2:
                            m_heating_type = st.selectbox("Heating Type", cols, index=_idx("heating_type"), key=f"heating_type_{idx}")
                        
                        st.markdown("**Owner Info**")
                        col1, col2, col3, col4 = st.columns(4)
                        with col1:
                            m_ownership_months = st.selectbox("Ownership Length (Months)", cols, index=_idx("ownership_length_months"), key=f"ownership_months_{idx}")
                        with col2:
                            m_owner_type = st.selectbox("Owner Type", cols, index=_idx("owner_type"), key=f"owner_type_{idx}")
                        with col3:
                            m_owner_occupied = st.selectbox("Owner Occupied", cols, index=_idx("owner_occupied"), key=f"owner_occupied_{idx}")
                        with col4:
                            m_vacant = st.selectbox("Vacant?", cols, index=_idx("vacant"), key=f"vacant_{idx}")
                        
                        st.markdown("**Financial**")
                        col1, col2, col3 = st.columns(3)
                        with col1:
                            m_occ = st.selectbox("Occupancy", cols, index=_idx("occupancy"), key=f"occ_{idx}")
                        with col2:
                            m_value = st.selectbox("Est Value", cols, index=_idx("est_value"), key=f"val_{idx}")
                        with col3:
                            m_sale_price = st.selectbox("Last Sale Price", cols, index=_idx("last_sale_price"), key=f"sale_{idx}")

                    essential_ok = (
                        m_prop_addr != "None"
                        and m_prop_city != "None"
                        and m_prop_state != "None"
                        and (m_first_name != "None" or m_last_name != "None")
                    )
                    if not essential_ok:
                        st.warning("Map at least: Property Address, Property City, Property State, and First Name or Last Name.")
                    else:
                        if st.button(f"Import this file", key=f"import_{idx}", type="primary"):
                            prog = st.progress(0)
                            status = st.empty()
                            err_container = st.container()
                            stats = {"new": 0, "error": 0, "skipped": 0}

                            for i, row in raw_df.iterrows():
                                try:
                                    addr_val = row.get(m_prop_addr) if m_prop_addr != "None" else None
                                    if pd.isna(addr_val) or str(addr_val).strip() == '':
                                        stats["skipped"] += 1
                                        continue

                                    city_val = (str(row.get(m_prop_city, "")).strip()[:100]) if m_prop_city != "None" else ""
                                    state_val = (str(row.get(m_prop_state, "")).strip().upper()[:2]) if m_prop_state != "None" else ""
                                    if not city_val or not state_val:
                                        stats["skipped"] += 1
                                        continue

                                    payload = {
                                        "address": str(addr_val).strip()[:255],
                                        "city": city_val,
                                        "state": state_val,
                                    }
                                    if source_name:
                                        payload["source"] = source_name

                                    # Property Zip, County
                                    if m_prop_zip != "None" and not pd.isna(row.get(m_prop_zip)):
                                        payload["zip"] = str(row[m_prop_zip]).strip()[:20]
                                    if m_prop_county != "None" and not pd.isna(row.get(m_prop_county)):
                                        payload["county"] = str(row[m_prop_county]).strip()[:100]

                                    # Owner 1 First Name, Last Name
                                    owner_first = ""
                                    owner_last = ""
                                    if m_first_name != "None" and not pd.isna(row.get(m_first_name)):
                                        owner_first = str(row[m_first_name]).strip()
                                        payload["owner_first"] = owner_first
                                    if m_last_name != "None" and not pd.isna(row.get(m_last_name)):
                                        owner_last = str(row[m_last_name]).strip()
                                        payload["owner_last"] = owner_last
                                    
                                    # Additional Owners (2-4) - combine into owner_name if multiple owners
                                    owner_names_parts = []
                                    owner1_name = f"{owner_first} {owner_last}".strip()
                                    if owner1_name:
                                        owner_names_parts.append(owner1_name)
                                    
                                    if m_owner2_first != "None" and not pd.isna(row.get(m_owner2_first)):
                                        owner2_first = str(row[m_owner2_first]).strip()
                                        owner2_last = str(row.get(m_owner2_last, "")).strip() if m_owner2_last != "None" and not pd.isna(row.get(m_owner2_last)) else ""
                                        owner2_name = f"{owner2_first} {owner2_last}".strip()
                                        if owner2_name:
                                            owner_names_parts.append(owner2_name)
                                    if m_owner3_first != "None" and not pd.isna(row.get(m_owner3_first)):
                                        owner3_first = str(row[m_owner3_first]).strip()
                                        owner3_last = str(row.get(m_owner3_last, "")).strip() if m_owner3_last != "None" and not pd.isna(row.get(m_owner3_last)) else ""
                                        owner3_name = f"{owner3_first} {owner3_last}".strip()
                                        if owner3_name:
                                            owner_names_parts.append(owner3_name)
                                    if m_owner4_first != "None" and not pd.isna(row.get(m_owner4_first)):
                                        owner4_first = str(row[m_owner4_first]).strip()
                                        owner4_last = str(row.get(m_owner4_last, "")).strip() if m_owner4_last != "None" and not pd.isna(row.get(m_owner4_last)) else ""
                                        owner4_name = f"{owner4_first} {owner4_last}".strip()
                                        if owner4_name:
                                            owner_names_parts.append(owner4_name)
                                    
                                    # If multiple owners, set owner_name directly (core.py will use it)
                                    if len(owner_names_parts) > 1:
                                        payload["owner_name"] = " / ".join(owner_names_parts)
                                        # Clear owner_first/owner_last so core.py uses owner_name instead
                                        payload.pop("owner_first", None)
                                        payload.pop("owner_last", None)

                                    # Mailing Address, City, State, Zip
                                    if m_mail_addr != "None" and not pd.isna(row.get(m_mail_addr)):
                                        payload["mailing_address"] = str(row[m_mail_addr]).strip()[:255]
                                    if m_mail_city != "None" and not pd.isna(row.get(m_mail_city)):
                                        payload["mailing_city"] = str(row[m_mail_city]).strip()[:100]
                                    if m_mail_state != "None" and not pd.isna(row.get(m_mail_state)):
                                        payload["mailing_state"] = str(row[m_mail_state]).strip()[:2]
                                    if m_mail_zip != "None" and not pd.isna(row.get(m_mail_zip)):
                                        payload["mailing_zip"] = str(row[m_mail_zip]).strip()[:20]

                                    # Phone 1, 2, 3, 4
                                    phones = []
                                    for ph in [m_phone1, m_phone2, m_phone3, m_phone4]:
                                        if ph != "None" and not pd.isna(row.get(ph)) and str(row[ph]).strip():
                                            phones.append(str(row[ph]).strip())
                                    if phones:
                                        payload["phone_numbers"] = ", ".join(phones)

                                    # Property Details
                                    if m_apn != "None" and not pd.isna(row.get(m_apn)):
                                        payload["apn"] = str(row[m_apn]).strip()[:50]
                                    if m_prop_type != "None" and not pd.isna(row.get(m_prop_type)):
                                        payload["property_type"] = str(row[m_prop_type]).strip()
                                    if m_prop_use != "None" and not pd.isna(row.get(m_prop_use)):
                                        payload["property_use"] = str(row[m_prop_use]).strip()
                                    if m_land_use != "None" and not pd.isna(row.get(m_land_use)):
                                        payload["land_use"] = str(row[m_land_use]).strip()
                                    if m_subdivision != "None" and not pd.isna(row.get(m_subdivision)):
                                        payload["subdivision"] = str(row[m_subdivision]).strip()[:200]
                                    if m_legal_desc != "None" and not pd.isna(row.get(m_legal_desc)):
                                        payload["legal_description"] = str(row[m_legal_desc]).strip()[:500]
                                    
                                    # Property Size & Structure
                                    if m_living_sqft != "None" and not pd.isna(row.get(m_living_sqft)):
                                        try:
                                            payload["living_sqft"] = int(float(str(row[m_living_sqft]).replace(',', '').strip()))
                                        except Exception:
                                            pass
                                    if m_lot_acres != "None" and not pd.isna(row.get(m_lot_acres)):
                                        try:
                                            payload["lot_acres"] = float(str(row[m_lot_acres]).replace(',', '').strip())
                                        except Exception:
                                            pass
                                    if m_lot_sqft != "None" and not pd.isna(row.get(m_lot_sqft)):
                                        try:
                                            payload["lot_sqft"] = int(float(str(row[m_lot_sqft]).replace(',', '').strip()))
                                        except Exception:
                                            pass
                                    if m_year_built != "None" and not pd.isna(row.get(m_year_built)):
                                        try:
                                            payload["year_built"] = int(float(str(row[m_year_built]).strip()))
                                        except Exception:
                                            pass
                                    if m_beds != "None" and not pd.isna(row.get(m_beds)):
                                        try:
                                            payload["beds"] = int(float(row[m_beds]))
                                        except Exception:
                                            pass
                                    if m_baths != "None" and not pd.isna(row.get(m_baths)):
                                        try:
                                            payload["baths"] = float(row[m_baths])
                                        except Exception:
                                            pass
                                    if m_stories != "None" and not pd.isna(row.get(m_stories)):
                                        try:
                                            payload["stories"] = int(float(row[m_stories]))
                                        except Exception:
                                            pass
                                    if m_units_count != "None" and not pd.isna(row.get(m_units_count)):
                                        try:
                                            payload["units_count"] = int(float(row[m_units_count]))
                                        except Exception:
                                            pass
                                    if m_fireplaces != "None" and not pd.isna(row.get(m_fireplaces)):
                                        try:
                                            payload["fireplaces"] = int(float(row[m_fireplaces]))
                                        except Exception:
                                            pass
                                    
                                    # Garage & Carport
                                    if m_garage_type != "None" and not pd.isna(row.get(m_garage_type)):
                                        payload["garage_type"] = str(row[m_garage_type]).strip()[:50]
                                    if m_garage_sqft != "None" and not pd.isna(row.get(m_garage_sqft)):
                                        try:
                                            payload["garage_sqft"] = int(float(str(row[m_garage_sqft]).replace(',', '').strip()))
                                        except Exception:
                                            pass
                                    if m_carport != "None" and not pd.isna(row.get(m_carport)):
                                        carport_val = str(row[m_carport]).strip().lower()
                                        payload["carport"] = carport_val in ['yes', 'y', 'true', '1', 'has carport']
                                    if m_carport_area != "None" and not pd.isna(row.get(m_carport_area)):
                                        try:
                                            payload["carport_area"] = int(float(str(row[m_carport_area]).replace(',', '').strip()))
                                        except Exception:
                                            pass
                                    
                                    # HVAC
                                    if m_ac_type != "None" and not pd.isna(row.get(m_ac_type)):
                                        payload["ac_type"] = str(row[m_ac_type]).strip()[:50]
                                    if m_heating_type != "None" and not pd.isna(row.get(m_heating_type)):
                                        payload["heating_type"] = str(row[m_heating_type]).strip()[:50]
                                    
                                    # Owner Info
                                    if m_ownership_months != "None" and not pd.isna(row.get(m_ownership_months)):
                                        try:
                                            payload["ownership_length_months"] = int(float(row[m_ownership_months]))
                                        except Exception:
                                            pass
                                    if m_owner_type != "None" and not pd.isna(row.get(m_owner_type)):
                                        payload["owner_type"] = str(row[m_owner_type]).strip()[:50]
                                    if m_owner_occupied != "None" and not pd.isna(row.get(m_owner_occupied)):
                                        occupied_val = str(row[m_owner_occupied]).strip().lower()
                                        payload["owner_occupied"] = occupied_val in ['yes', 'y', 'true', '1', 'owner occupied']
                                    if m_vacant != "None" and not pd.isna(row.get(m_vacant)):
                                        vacant_val = str(row[m_vacant]).strip().lower()
                                        payload["vacant"] = vacant_val in ['yes', 'y', 'true', '1', 'vacant']
                                    
                                    # Financial
                                    if m_occ != "None" and not pd.isna(row.get(m_occ)):
                                        occ_val = str(row[m_occ]).strip()
                                        if occ_val.lower() in ['vacant', 'v', 'empty']:
                                            payload["occupancy_status"] = "Vacant"
                                        elif occ_val.lower() in ['occupied', 'occ', 'owner occupied']:
                                            payload["occupancy_status"] = "Occupied"
                                        else:
                                            payload["occupancy_status"] = occ_val
                                    if m_value != "None" and not pd.isna(row.get(m_value)):
                                        try:
                                            val = str(row[m_value]).replace('$', '').replace(',', '').strip()
                                            payload["est_value"] = float(val)
                                        except Exception:
                                            pass
                                    if m_sale_price != "None" and not pd.isna(row.get(m_sale_price)):
                                        try:
                                            val = str(row[m_sale_price]).replace('$', '').replace(',', '').strip()
                                            payload["last_sale_price"] = float(val)
                                        except Exception:
                                            pass

                                    # Insert (no duplicate check)
                                    stack_lead(payload)
                                    stats["new"] += 1
                                except Exception as e:
                                    stats["error"] += 1
                                    with err_container:
                                        st.error(f"Row {i+2}: {str(e)[:100]}")
                                prog.progress((i + 1) / len(raw_df))
                                status.text(f"Processed {i+1}/{len(raw_df)}")

                            st.success(f"✅ New: {stats['new']}, Errors: {stats['error']}, Skipped: {stats['skipped']}")
                            if stats['new'] > 0:
                                st.balloons()
                                try:
                                    list_name = source_name.strip() if (source_name and source_name.strip()) else uploaded_file.name
                                    add_uploaded_list(list_name, uploaded_file.name)
                                except Exception:
                                    pass
                except Exception as e:
                    st.error(f"Error reading file: {e}")

# ==================================================
# TAB: MY FILES
# ==================================================
with tab_files:
    st.header("📁 My files")
    st.caption("Uploaded lists and their status. Use the checkbox to select and delete, or change status and click Update.")
    try:
        files = list_uploaded_lists()
        if not files:
            st.info("No uploaded files yet. Import a CSV in **Bulk Import** to see it here.")
        else:
            # Toolbar: Delete selected
            if st.button("Delete selected", type="secondary", key="myfiles_delete_btn"):
                selected_ids = []
                for f in files:
                    if st.session_state.get(f"myfiles_sel_{f['id']}", False):
                        selected_ids.append(f["id"])
                if selected_ids:
                    delete_uploaded_lists(selected_ids)
                    st.success(f"Deleted {len(selected_ids)} file(s).")
                    st.rerun()
                else:
                    st.warning("Select at least one file (checkbox) to delete.")

            st.divider()
            for f in files:
                chk, col_name, col_date, col_leads, col_status, col_btn = st.columns([0.4, 2, 1.5, 0.8, 1.5, 0.6])
                with chk:
                    st.checkbox("Select", value=False, key=f"myfiles_sel_{f['id']}", label_visibility="collapsed")
                with col_name:
                    st.markdown(f"**{f['name']}**")
                    st.caption(f"📄 {f['filename']}")
                with col_date:
                    st.caption(f"{f['uploaded_at']}")
                with col_leads:
                    st.caption(f"{f['lead_count']} leads")
                with col_status:
                    new_status = st.selectbox(
                        "Status",
                        UPLOAD_STATUSES,
                        index=UPLOAD_STATUSES.index(f["status"]) if f["status"] in UPLOAD_STATUSES else 0,
                        key=f"file_status_{f['id']}",
                    )
                with col_btn:
                    if st.button("Update", key=f"file_update_{f['id']}"):
                        update_uploaded_list_status(f["id"], new_status)
                        st.success("Updated.")
                        st.rerun()
                st.divider()
    except Exception as e:
        st.error(str(e))
        st.exception(e)

# ==================================================
# TAB: TAG MANAGER
# ==================================================
with tab4:
    st.header("🏷 Tag manager")
    st.caption("View all tags, see which leads have a tag, and rename or remove tags.")
    if "tags" not in _cols:
        st.warning("Your `properties` table has no `tags` column. Add it with: `ALTER TABLE properties ADD COLUMN tags TEXT;`")
    else:
        try:
            tag_list = get_all_tags_with_counts()
            if not tag_list:
                st.info("No tags yet. Add tags from **Lead Engine** (batch Tag) or when importing.")
            else:
                st.subheader("All tags")
                tag_df = pd.DataFrame(tag_list)
                st.dataframe(tag_df, use_container_width=True, hide_index=True)
                st.divider()
                tag_names = [t["tag_name"] for t in tag_list]
                chosen = st.selectbox("Select a tag", tag_names, key="tag_manager_choose")
                chosen_count = next((t["cnt"] for t in tag_list if t["tag_name"] == chosen), 0)
                # Clear shown tag if user picked a different tag
                if st.session_state.get("tag_manager_show_tag") != chosen:
                    st.session_state.pop("tag_manager_show_tag", None)

                # Show leads with this tag
                st.subheader(f"Leads with tag «{chosen}»")
                if st.button("View leads with this tag", key="tag_show_leads_btn", type="primary"):
                    st.session_state["tag_manager_show_tag"] = chosen
                    st.rerun()
                if st.session_state.get("tag_manager_show_tag") == chosen:
                    leads = get_leads_by_tag(chosen)
                    if not leads:
                        st.info("No leads found with this tag.")
                    else:
                        lead_df = pd.DataFrame(leads)
                        st.caption(f"**{len(lead_df)}** lead(s) with tag «{chosen}».")
                        st.dataframe(lead_df, use_container_width=True, hide_index=True)
                else:
                    st.caption(f"Click **Show leads** to view the **{chosen_count}** lead(s) that have this tag.")

                st.divider()
                st.subheader("Rename or remove tag")
                col_rename, col_remove = st.columns(2)
                with col_rename:
                    new_name = st.text_input("New name (rename)", placeholder="e.g. hot-lead → hot", key="tag_new_name")
                    if st.button("Rename tag", key="tag_rename_btn") and new_name and new_name.strip():
                        if new_name.strip().lower() == chosen.lower():
                            st.warning("New name is the same as current.")
                        else:
                            n = rename_tag(chosen, new_name.strip())
                            st.success(f"Renamed in {n} lead(s).")
                            st.rerun()
                with col_remove:
                    if st.button("Remove tag from all leads", key="tag_remove_btn", type="secondary"):
                        n = remove_tag_from_all(chosen)
                        st.success(f"Removed from {n} lead(s).")
                        st.rerun()
        except Exception as e:
            st.error(str(e))
            st.exception(e)

# ==================================================
# SIDEBAR
# ==================================================
st.sidebar.markdown("---")
st.sidebar.markdown("**Data**")
with st.sidebar.expander("Clear properties", expanded=False):
    in_confirm = st.session_state.get("clear_confirm") is not None
    if not in_confirm:
        clear_scope = st.selectbox(
            "Scope",
            ["All states"] + sorted(all_states),
            key="clear_scope",
        )
        state_to_clear = None if clear_scope == "All states" else clear_scope
        count = count_properties(state_to_clear)
        st.caption(f"{count:,} properties would be deleted.")
        if st.button("Clear data", type="secondary", key="clear_data_btn"):
            st.session_state["clear_confirm"] = state_to_clear
            st.rerun()
    else:
        confirm_state = st.session_state["clear_confirm"]
        label = "all states" if confirm_state is None else f"state **{confirm_state}**"
        try:
            count = count_properties(confirm_state)
            st.warning(f"Delete **{count:,}** properties ({label})? This cannot be undone.")
        except Exception as e:
            st.error(f"Error counting properties: {str(e)}")
            count = 0
        c1, c2 = st.columns(2)
        with c1:
            if st.button("Yes, delete", key="clear_confirm_yes"):
                try:
                    delete_properties(confirm_state)
                    st.session_state.pop("clear_confirm", None)
                    st.success(f"Cleared {count:,} properties.")
                    st.rerun()
                except Exception as e:
                    st.error(f"Error clearing data: {str(e)}")
                    st.exception(e)
        with c2:
            if st.button("Cancel", key="clear_confirm_no"):
                st.session_state.pop("clear_confirm", None)
                st.rerun()

st.sidebar.markdown("**Developer**")
with st.sidebar.expander("Schema & stats", expanded=False):
    if st.button("Check schema"):
        schema = get_table_schema()
        if schema:
            st.dataframe(pd.DataFrame(schema))
            cnt = pd.read_sql("SELECT COUNT(*) FROM properties", get_db_connection())
            st.write(f"Total records: {cnt.iloc[0,0]}")