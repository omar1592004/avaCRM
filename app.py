import streamlit as st
import pandas as pd
import datetime
import json
from core import (
    execute_query,
    get_db_connection,
    stack_lead,
    bulk_insert_leads,
    get_table_schema,
    get_pipeline_counts,
    get_leads_by_stage,
    update_stage,
    add_lead_activity,
    get_lead_activities,
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
    # ── Upgrades ──
    get_stacked_leads,
    get_list_stack_summary,
    calculate_distress_score,
    batch_update_distress_scores,
    get_score_distribution,
    skip_trace_lead,
    bulk_skip_trace,
    get_leads_with_coords,
    ensure_lat_lon_columns,
    batch_geocode,
    get_view_kpis,
)

# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────
def _norm(s):
    if s is None or pd.isna(s):
        return ""
    return " ".join(str(s).strip().lower().replace("_", " ").replace("-", " ").split())

DEFAULT_IMPORT_MAP = {
    "property_address":    ["property address","address","street address","prop address","situs address","street"],
    "property_city":       ["property city","city","prop city","property city name"],
    "property_state":      ["property state","state","prop state","st","property sta"],
    "property_zip":        ["property zip","zip","zip code","zipcode","prop zip"],
    "property_county":     ["property county","county","prop county"],
    "first_name":          ["first name","firstname","first_name","owner first","owner first name","owner 1 first name","fname"],
    "last_name":           ["last name","lastname","last_name","owner last","owner last name","owner 1 last name","lname"],
    "owner_2_first_name":  ["owner 2 first name","owner2 first name","owner 2 first"],
    "owner_2_last_name":   ["owner 2 last name","owner2 last name","owner 2 last"],
    "owner_3_first_name":  ["owner 3 first name","owner3 first name"],
    "owner_3_last_name":   ["owner 3 last name","owner3 last name"],
    "owner_4_first_name":  ["owner 4 first name","owner4 first name"],
    "owner_4_last_name":   ["owner 4 last name","owner4 last name"],
    "mailing_address":     ["mailing address","mail address","owner address","owner mailing address"],
    "mailing_city":        ["mailing city","mail city","owner mailing city"],
    "mailing_state":       ["mailing state","mail state","owner mailing state"],
    "mailing_zip":         ["mailing zip","mail zip","owner mailing zip"],
    "phone_1":             ["phone 1","phone","cell","mobile","telephone","primary phone","owner phone"],
    "phone_2":             ["phone 2","secondary phone","alt phone"],
    "phone_3":             ["phone 3"],
    "phone_4":             ["phone 4"],
    "apn":                 ["apn","parcel","parcel id","parcel number"],
    "property_type":       ["property type","type","prop type"],
    "property_use":        ["property use","use","prop use"],
    "land_use":            ["land use","land use code"],
    "subdivision":         ["subdivision","sub","subdivision name"],
    "legal_description":   ["legal description","legal desc","legal"],
    "living_sqft":         ["living square feet","living sqft","living sq ft","sqft","square feet","living area"],
    "lot_acres":           ["lot acres","acres","lot size acres"],
    "lot_sqft":            ["lot square feet","lot sqft","lot sq ft","lot size"],
    "year_built":          ["year built","built","year","construction year"],
    "stories":             ["stories","# of stories","number of stories","floors"],
    "units_count":         ["units count","units","number of units","# of units"],
    "beds":                ["beds","bedrooms","bed","br"],
    "baths":               ["baths","bathrooms","bath","ba"],
    "fireplaces":          ["fireplaces","# of fireplaces"],
    "ac_type":             ["air conditioning type","ac type","air conditioning","ac","cooling type"],
    "heating_type":        ["heating type","heating","heat type"],
    "garage_type":         ["garage type","garage"],
    "garage_sqft":         ["garage square feet","garage sqft","garage sq ft"],
    "carport":             ["carport","has carport"],
    "carport_area":        ["carport area","carport sqft"],
    "ownership_length_months": ["ownership length months","ownership length","months owned"],
    "owner_type":          ["owner type","owner type code"],
    "owner_occupied":      ["owner occupied","owner occ","occupied"],
    "vacant":              ["vacant","is vacant","vacancy"],
    "occupancy":           ["occupancy","occupancy status","occ"],
    "est_value":           ["est value","estimated value","value","market value","avm"],
    "last_sale_price":     ["last sale price","last sale","sale price","sales price","sold price"],
}

def _match_column(csv_columns, field_key, used=None):
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

# ─────────────────────────────────────────────
# PAGE CONFIG
# ─────────────────────────────────────────────
st.set_page_config(
    layout="wide",
    page_title="RE Engine Pro",
    initial_sidebar_state="expanded",
    page_icon="🏠",
)

# ─────────────────────────────────────────────
# GLOBAL CSS — layout & custom components only
# (theme colors handled by .streamlit/config.toml)
# ─────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');

html, body, [class*="css"], [class*="st-"] {
    font-family: 'Inter', sans-serif !important;
}

/* ── Layout ── */
.main .block-container {
    padding: 0.5rem 1.5rem 3rem 1.5rem !important;
    max-width: 100% !important;
}

/* ── Hide sidebar entirely — using top nav ── */
[data-testid="stSidebar"],
[data-testid="stSidebarCollapseButton"],
[data-testid="stSidebarNav"],
button[kind="header"] { display: none !important; }

/* ── Top nav button row ── */
div[data-testid="stHorizontalBlock"] > div[data-testid="stColumn"] .stButton > button {
    border-radius: 6px !important;
    font-size: 0.82rem !important;
    font-weight: 600 !important;
    padding: 0.35rem 0.5rem !important;
    width: 100% !important;
}
div[data-testid="stHorizontalBlock"] > div[data-testid="stColumn"] .stButton > button[kind="primary"] {
    background: #2f81f7 !important;
    border-color: #2f81f7 !important;
    color: #fff !important;
}
div[data-testid="stHorizontalBlock"] > div[data-testid="stColumn"] .stButton > button[kind="secondary"] {
    background: #161b22 !important;
    border-color: #30363d !important;
    color: #8b949e !important;
}



/* ── Custom metric tiles ── */
.metric-tile {
    background: rgba(255,255,255,0.04);
    border: 1px solid rgba(255,255,255,0.08);
    border-radius: 8px;
    padding: 1.1rem 1.4rem;
}
.metric-tile .label {
    font-size: 0.72rem; font-weight: 600; letter-spacing: 0.06em;
    text-transform: uppercase; opacity: 0.55; margin-bottom: 0.3rem;
}
.metric-tile .value { font-size: 1.85rem; font-weight: 700; line-height: 1.1; }
.metric-tile .delta { font-size: 0.78rem; color: #3fb950; margin-top: 0.25rem; }
.metric-tile .delta.down { color: #f85149; }

/* ── Pipeline board ── */
.pipeline-col {
    background: rgba(255,255,255,0.04);
    border: 1px solid rgba(255,255,255,0.08);
    border-radius: 8px;
    padding: 0.85rem 1rem;
    min-height: 80px;
}
.pipeline-header {
    font-size: 0.72rem; font-weight: 700; letter-spacing: 0.07em;
    text-transform: uppercase; margin-bottom: 0.5rem;
    padding-bottom: 0.5rem; border-bottom: 2px solid;
}
.pipeline-count { font-size: 1.6rem; font-weight: 700; }
.pipeline-sub   { font-size: 0.75rem; opacity: 0.5; }

.stage-new         { color: #58a6ff !important; border-color: #58a6ff !important; }
.stage-contacted   { color: #bc8cff !important; border-color: #bc8cff !important; }
.stage-negotiating { color: #e3b341 !important; border-color: #e3b341 !important; }
.stage-closed      { color: #3fb950 !important; border-color: #3fb950 !important; }
.stage-lost        { color: #f85149 !important; border-color: #f85149 !important; }
.stage-unset       { color: #484f58 !important; border-color: #484f58 !important; }

/* ── Cards ── */
.re-card {
    background: rgba(255,255,255,0.04);
    border: 1px solid rgba(255,255,255,0.08);
    border-radius: 8px;
    padding: 1.25rem 1.5rem;
    margin-bottom: 1rem;
}

/* ── Section / page titles ── */
.section-title {
    font-size: 1rem; font-weight: 700;
    margin: 0 0 1rem 0; padding-bottom: 0.5rem;
    border-bottom: 1px solid rgba(255,255,255,0.08);
}
.page-title  { font-size: 1.5rem; font-weight: 700; margin-bottom: 0.15rem; }
.page-sub    { font-size: 0.85rem; opacity: 0.55; margin-bottom: 1.25rem; }

/* ── Filter labels ── */
.filter-section {
    font-size: 0.7rem; font-weight: 700; letter-spacing: 0.07em;
    text-transform: uppercase; opacity: 0.5; margin: 1rem 0 0.4rem 0;
}

/* ── Import step badges ── */
.step-badge {
    display: inline-flex; align-items: center; justify-content: center;
    width: 26px; height: 26px; background: #2f81f7; color: #fff;
    border-radius: 50%; font-size: 0.75rem; font-weight: 700;
    margin-right: 0.5rem; flex-shrink: 0;
}
.step-row {
    display: flex; align-items: center; margin-bottom: 0.5rem;
    font-size: 0.875rem; font-weight: 500;
}

/* ── Scrollbar ── */
::-webkit-scrollbar { width: 6px; height: 6px; }
::-webkit-scrollbar-thumb { background: rgba(255,255,255,0.12); border-radius: 3px; }
::-webkit-scrollbar-thumb:hover { background: rgba(255,255,255,0.2); }

/* Hide Streamlit chrome */
#MainMenu, footer, header { visibility: hidden; }
</style>
""", unsafe_allow_html=True)

# Force sidebar always open via JS
st.markdown('''
<script>
// Force sidebar expanded on load
const forceOpenSidebar = () => {
    const btn = window.parent.document.querySelector('[data-testid="stSidebarCollapseButton"]');
    const sidebar = window.parent.document.querySelector('[data-testid="stSidebar"]');
    if (sidebar) {
        sidebar.style.display = "block";
        sidebar.style.visibility = "visible";
        sidebar.style.width = "240px";
        sidebar.style.minWidth = "240px";
    }
};
document.addEventListener("DOMContentLoaded", forceOpenSidebar);
setTimeout(forceOpenSidebar, 100);
setTimeout(forceOpenSidebar, 500);
</script>
''', unsafe_allow_html=True)


# ─────────────────────────────────────────────
# DB INIT
# ─────────────────────────────────────────────
_db_ok = True
try:
    _schema     = get_table_schema()
    _cols       = [r["column_name"] for r in _schema] if _schema else []
    STATE_COL   = "state" if "state" in _cols else "property_state"
    _all_states = set()
    for col in ["state", "property_state"]:
        if col not in _cols:
            continue
        try:
            rows = execute_query(
                f"SELECT DISTINCT TRIM({col}) AS v FROM properties "
                f"WHERE {col} IS NOT NULL AND TRIM({col}) != '' ORDER BY v",
                fetch=True,
            )
            if rows:
                for r in rows:
                    if r.get("v"):
                        _all_states.add(r["v"].strip())
        except Exception:
            pass
    all_states = sorted(_all_states)
except Exception:
    _db_ok     = False
    _schema    = []
    _cols      = []
    STATE_COL  = "state"
    all_states = []

# ─────────────────────────────────────────────
# SIDEBAR NAV
# ─────────────────────────────────────────────
# ── TOP NAV BAR ──
if "current_page" not in st.session_state:
    st.session_state["current_page"] = "Dashboard"

PAGES = ["Dashboard", "Lead Engine", "Pipeline", "Import", "My Files", "Tags"]
ICONS = {"Dashboard":"📈","Lead Engine":"🔍","Pipeline":"📊","Import":"📥","My Files":"📁","Tags":"🏷"}

st.markdown("""
<div style="display:flex;align-items:center;justify-content:space-between;
     padding:0.6rem 1.5rem;background:#161b22;border-bottom:1px solid #30363d;
     margin:-1rem -2rem 1.5rem -2rem;position:sticky;top:0;z-index:999;">
  <div style="font-size:1rem;font-weight:700;color:#e6edf3;white-space:nowrap;">
    🏠 RE Engine Pro
  </div>
</div>
""", unsafe_allow_html=True)

nav_cols = st.columns(len(PAGES))
for i, pg in enumerate(PAGES):
    active = st.session_state["current_page"] == pg
    with nav_cols[i]:
        label = f"{ICONS[pg]} {pg}"
        if st.button(label, key=f"topnav_{pg}", use_container_width=True,
                     type="primary" if active else "secondary"):
            st.session_state["current_page"] = pg
            st.rerun()

st.markdown("<div style='border-bottom:1px solid #30363d;margin-bottom:1.25rem;'></div>",
            unsafe_allow_html=True)

page_key = st.session_state["current_page"]

# Market filter — shown inline on relevant pages
selected_state = "All States"
if page_key in ("Lead Engine", "Pipeline", "Dashboard"):
    _sf_col, _ = st.columns([2, 5])
    with _sf_col:
        selected_state = st.selectbox(
            "🌎 Market", ["All States"] + all_states,
            key="top_market", label_visibility="visible"
        )

# Data management in a small expander at bottom (only on Dashboard)
if page_key == "Dashboard":
    with st.expander("⚙️ Data Management", expanded=False):
        clear_scope = st.selectbox("Scope", ["All states"] + sorted(all_states), key="clear_scope")
        state_to_clear = None if clear_scope == "All states" else clear_scope
        count_c = 0
        try:
            count_c = count_properties(state_to_clear)
        except Exception:
            pass
        st.caption(f"{count_c:,} properties in scope")
        if st.button("🗑 Clear data", type="secondary", key="clear_data_btn"):
            st.session_state["clear_confirm"] = state_to_clear
            st.rerun()
        if st.session_state.get("clear_confirm") is not None:
            confirm_state = st.session_state["clear_confirm"]
            label_text = "all states" if confirm_state is None else confirm_state
            st.warning(f"Delete {count_c:,} ({label_text})?")
            c1, c2 = st.columns(2)
            with c1:
                if st.button("Yes", key="clear_yes"):
                    try:
                        delete_properties(confirm_state)
                        st.session_state.pop("clear_confirm", None)
                        st.success("Cleared.")
                        st.rerun()
                    except Exception as e:
                        st.error(str(e))
            with c2:
                if st.button("No", key="clear_no"):
                    st.session_state.pop("clear_confirm", None)
                    st.rerun()

if not _db_ok:
    st.error("**Cannot connect to the database.** Check your secrets / environment variables.")
    st.stop()

# ─────────────────────────────────────────────
# CURRENT PAGE
# ─────────────────────────────────────────────
# page_key set by top nav above

# ═══════════════════════════════════════════════════════════════
# PAGE: DASHBOARD
# ═══════════════════════════════════════════════════════════════
if page_key == "Dashboard":
    st.markdown('<div class="page-title">Dashboard</div>', unsafe_allow_html=True)
    st.markdown('<div class="page-sub">Live overview of your lead database and pipeline activity.</div>', unsafe_allow_html=True)

    try:
        stats    = get_dashboard_stats()
        total    = stats["total"]
        by_state = stats["by_state"]
        by_stage = stats["by_stage"]

        stage_map = {r["stage"]: r["cnt"] for r in by_stage} if by_stage else {}
        total_pipeline = sum(stage_map.values())

        # ── KPI row ──
        k1, k2, k3, k4, k5 = st.columns(5)
        def kpi(col, label, value, delta=None, down=False):
            delta_html = ""
            if delta:
                cls = "down" if down else ""
                delta_html = f'<div class="delta {cls}">{delta}</div>'
            col.markdown(f"""
            <div class="metric-tile">
                <div class="label">{label}</div>
                <div class="value">{value}</div>
                {delta_html}
            </div>""", unsafe_allow_html=True)

        kpi(k1, "Total Leads",      f"{total:,}")
        kpi(k2, "States",           str(len(by_state)) if by_state else "0")
        kpi(k3, "In Pipeline",      f"{total_pipeline:,}")
        kpi(k4, "Closed",           f"{stage_map.get('Closed', 0):,}", delta="✓ Won")
        kpi(k5, "Motivation Score", "—")

        st.markdown("<div style='margin-top:1.5rem;'></div>", unsafe_allow_html=True)

        # ── Pipeline summary bar ──
        st.markdown('<div class="section-title">Pipeline Overview</div>', unsafe_allow_html=True)
        PIPELINE_STAGES = ["New", "Contacted", "Negotiating", "Closed", "Lost", "Unset"]
        STAGE_COLORS    = {
            "New": "#3498db", "Contacted": "#9b59b6", "Negotiating": "#f39c12",
            "Closed": "#27ae60", "Lost": "#e74c3c", "Unset": "#95a5a6",
        }
        STAGE_CSS = {
            "New": "stage-new", "Contacted": "stage-contacted", "Negotiating": "stage-negotiating",
            "Closed": "stage-closed", "Lost": "stage-lost", "Unset": "stage-unset",
        }
        pcols = st.columns(len(PIPELINE_STAGES))
        for i, s in enumerate(PIPELINE_STAGES):
            cnt = stage_map.get(s, 0)
            css = STAGE_CSS.get(s, "stage-unset")
            pcols[i].markdown(f"""
            <div class="pipeline-col">
                <div class="pipeline-header {css}">{s}</div>
                <div class="pipeline-count">{cnt:,}</div>
                <div class="pipeline-sub">leads</div>
            </div>""", unsafe_allow_html=True)

        st.markdown("<div style='margin-top:1.5rem;'></div>", unsafe_allow_html=True)

        # ── Charts row ──
        ch1, ch2 = st.columns(2)
        with ch1:
            st.markdown('<div class="section-title">Leads by State</div>', unsafe_allow_html=True)
            if by_state:
                df_s = pd.DataFrame(by_state).rename(columns={"state": "State", "cnt": "Leads"})
                st.bar_chart(df_s.set_index("State")["Leads"], height=220)
            else:
                st.info("No state data yet — import a list to populate.")
        with ch2:
            st.markdown('<div class="section-title">Pipeline Distribution</div>', unsafe_allow_html=True)
            if by_stage:
                df_p = pd.DataFrame(by_stage).rename(columns={"stage": "Stage", "cnt": "Leads"})
                st.bar_chart(df_p.set_index("Stage")["Leads"], height=220)
            else:
                st.info("No pipeline stages set yet.")

        st.markdown("<div style='margin-top:1.5rem;'></div>", unsafe_allow_html=True)

        # ── Recent uploads ──
        st.markdown('<div class="section-title">Recent Imports</div>', unsafe_allow_html=True)
        try:
            files = list_uploaded_lists()
            if files:
                file_df = pd.DataFrame(files[:10])[["name", "filename", "uploaded_at", "status", "lead_count"]]
                file_df.columns = ["List Name", "File", "Uploaded", "Status", "Leads"]
                st.dataframe(file_df, use_container_width=True, hide_index=True)
            else:
                st.info("No imports yet. Go to **Import** to upload your first list.")
        except Exception:
            st.info("No import history available.")

    except Exception as e:
        st.error(str(e))
        st.exception(e)

    # ── Distress Score Distribution + Stacking ── (outside try block)
    st.markdown("<div style='margin-top:1.5rem;'></div>", unsafe_allow_html=True)
    d1, d2 = st.columns([3, 1])
    with d1:
        st.markdown('<div class="section-title">🎯 Distress Score Distribution</div>', unsafe_allow_html=True)
        try:
            score_dist = get_score_distribution()
            if score_dist:
                df_sc = pd.DataFrame(score_dist).rename(columns={"score": "Score", "count": "Leads"})
                df_sc["Score"] = df_sc["Score"].astype(str)
                st.bar_chart(df_sc.set_index("Score")["Leads"], height=180)
            else:
                st.info("Go to Lead Engine → Distress Score → Recalculate All Scores to populate.")
        except Exception:
            st.info("No score data yet.")
    with d2:
        st.markdown('<div class="section-title">🔥 List Stacking</div>', unsafe_allow_html=True)
        try:
            stacked = get_stacked_leads(2)
            st.markdown(f'''<div class="metric-tile">
                <div class="label">Stacked Leads</div>
                <div class="value" style="color:#f85149;">{len(stacked):,}</div>
                <div class="delta">appear on 2+ lists</div>
            </div>''', unsafe_allow_html=True)
        except Exception:
            pass


# ═══════════════════════════════════════════════════════════════
# PAGE: LEAD ENGINE
# ═══════════════════════════════════════════════════════════════
elif page_key == "Lead Engine":
    st.markdown('<div class="page-title">Lead Engine</div>', unsafe_allow_html=True)
    st.markdown('<div class="page-sub">Search, filter, and act on your leads.</div>', unsafe_allow_html=True)

    # ── Layout: filters left, results right ──
    filter_col, results_col = st.columns([1, 3])

    with filter_col:
        st.markdown('<div class="re-card">', unsafe_allow_html=True)
        st.markdown("**Saved Searches**")
        saved_list = list_saved_searches()
        saved_options = {f"{r['name']}": r["id"] for r in saved_list}
        saved_choice = st.selectbox("Load a saved search", ["— Select —"] + list(saved_options.keys()), key="saved_search_choice")
        sc1, sc2 = st.columns(2)
        with sc1:
            load_clicked = st.button("Load", use_container_width=True, key="saved_load_btn")
        with sc2:
            delete_clicked = st.button("Delete", use_container_width=True, key="saved_delete_btn")
        save_name = st.text_input("Save current as", placeholder="e.g. OH absentee", key="saved_search_name")
        save_clicked = st.button("💾 Save search", use_container_width=True, key="saved_save_btn")
        st.markdown('</div>', unsafe_allow_html=True)

        # ── Filters card ──
        st.markdown('<div class="re-card">', unsafe_allow_html=True)
        st.markdown("**🏠 Property**")

        col1, col2 = st.columns(2)
        with col1:
            type_sf  = st.checkbox("Single-Family", key="prop_single")
            type_c   = st.checkbox("Condo",         key="prop_condo")
        with col2:
            type_m2  = st.checkbox("Multi (2-4)",   key="prop_multi2")
            type_m5  = st.checkbox("Multi (5+)",    key="prop_multi5")
        prop_types = ([t for t, c in [("Single-Family Homes", type_sf), ("Condo/Co-Ownerships", type_c),
                       ("Multi-Family (2-4)", type_m2), ("Multi-Family (5+)", type_m5)] if c])

        st.markdown('<div class="filter-section">Bedrooms</div>', unsafe_allow_html=True)
        f_beds   = st.radio("Beds",  ["Any","1+","2+","3+","4+","5+"], horizontal=True, key="beds_radio", label_visibility="collapsed")
        bed_value = 0 if f_beds == "Any" else int(f_beds.replace("+",""))

        st.markdown('<div class="filter-section">Bathrooms</div>', unsafe_allow_html=True)
        f_baths  = st.radio("Baths", ["Any","1+","2+","3+","4+","5+"], horizontal=True, key="baths_radio", label_visibility="collapsed")
        bath_value = 0 if f_baths == "Any" else int(f_baths.replace("+",""))

        st.markdown('<div class="filter-section">Occupancy</div>', unsafe_allow_html=True)
        occ_occ   = st.checkbox("Occupied", key="occ_occupied")
        occ_vac   = st.checkbox("Vacant",   key="occ_vacant")
        occupancy_list = [x for x, c in [("Occupied", occ_occ), ("Vacant", occ_vac)] if c]

        apn_search = st.text_input("APN", placeholder="Search APN...", key="apn_search")

        with st.expander("👤 Owner Filters"):
            owner_name_contains = st.text_input("Owner name contains", key="owner_name_search")
            ot1, ot2, ot3 = st.columns(3)
            with ot1: o_ind = st.checkbox("Individual", key="owner_indiv")
            with ot2: o_biz = st.checkbox("Business",   key="owner_biz")
            with ot3: o_bnk = st.checkbox("Bank/Trust", key="owner_bank")
            owner_types = [x for x, c in [("Individual", o_ind), ("Business", o_biz), ("Bank or Trust", o_bnk)] if c]
            is_absentee  = st.checkbox("Absentee Only", key="is_absentee")
            years_owned_min = st.number_input("Min years owned", min_value=0, value=0, step=1, key="years_owned")
            tax_year_max = st.number_input("Max tax year", min_value=2000, max_value=datetime.datetime.now().year,
                                           value=datetime.datetime.now().year, step=1, key="tax_year")

        with st.expander("💰 Financial Filters"):
            st.markdown('<div class="filter-section">Est. Value ($)</div>', unsafe_allow_html=True)
            ev1, ev2 = st.columns(2)
            with ev1: est_value_min = st.number_input("Min", min_value=0, value=0, step=10000, key="est_val_min", label_visibility="collapsed")
            with ev2: est_value_max = st.number_input("Max", min_value=0, value=0, step=10000, key="est_val_max", label_visibility="collapsed")

            st.markdown('<div class="filter-section">Est. Equity ($)</div>', unsafe_allow_html=True)
            ee1, ee2 = st.columns(2)
            with ee1: est_equity_min = st.number_input("Min", min_value=0, value=0, step=10000, key="est_eq_min", label_visibility="collapsed")
            with ee2: est_equity_max = st.number_input("Max", min_value=0, value=0, step=10000, key="est_eq_max", label_visibility="collapsed")

            st.markdown('<div class="filter-section">Equity %</div>', unsafe_allow_html=True)
            ep1, ep2 = st.columns(2)
            with ep1: est_equity_pct_min = st.number_input("Min%", min_value=0, max_value=100, value=0,   step=5, key="est_eq_pct_min", label_visibility="collapsed")
            with ep2: est_equity_pct_max = st.number_input("Max%", min_value=0, max_value=100, value=100, step=5, key="est_eq_pct_max", label_visibility="collapsed")

            st.markdown('<div class="filter-section">Last Sale Price ($)</div>', unsafe_allow_html=True)
            ls1, ls2 = st.columns(2)
            with ls1: last_sale_min = st.number_input("Min", min_value=0, value=0, step=10000, key="sale_min", label_visibility="collapsed")
            with ls2: last_sale_max = st.number_input("Max", min_value=0, value=0, step=10000, key="sale_max", label_visibility="collapsed")

            assessed_min = assessed_max = 0
            filter_by_sale_date = st.checkbox("Filter by last sale date", key="filter_sale_date")
            last_sale_date = st.date_input("Before", value=datetime.datetime.now(), key="sale_date", disabled=not filter_by_sale_date)
            private_loan = st.checkbox("Private Loan Only", key="private_loan")
            cash_buyer   = st.checkbox("Cash Buyer Only",   key="cash_buyer")

        with st.expander("📊 Address Appearances"):
            min_appearances = st.number_input("Min appearances", min_value=1, value=2, step=1, key="min_appear")
            show_only_multi = st.checkbox("Only multi-appearance addresses", value=False, key="show_multi")

        with st.expander("🎯 Distress Score"):
            min_distress = st.slider("Min Distress Score", 1, 10, 1, key="min_distress")
            filter_by_distress = st.checkbox("Filter by score", value=False, key="filter_distress")
            st.caption("Score = Absentee+Equity+Tax Delinquent+Vacant+Ownership length")
            if st.button("♻️ Recalculate All Scores", key="recalc_scores"):
                with st.spinner("Scoring leads..."):
                    try:
                        n = batch_update_distress_scores()
                        st.success(f"Updated {n:,} leads.")
                    except Exception as e:
                        st.error(str(e))

        st.markdown('</div>', unsafe_allow_html=True)
        run_search = st.button("🔍 Run Search", use_container_width=True, type="primary", key="run_search_btn")

    # ── Results ──
    with results_col:
        # ── Active filters summary ──
        active = []
        if selected_state != "All States": active.append(f"State: **{selected_state}**")
        if prop_types:     active.append("Types: " + ", ".join(prop_types))
        if bed_value:      active.append(f"Beds: {bed_value}+")
        if bath_value:     active.append(f"Baths: {bath_value}+")
        if occupancy_list: active.append("Occ: " + ", ".join(occupancy_list))
        if is_absentee:    active.append("Absentee only")
        if show_only_multi:active.append(f"Multi ≥{min_appearances}")
        if active:
            st.markdown("**Active filters:** " + "  ·  ".join(active))
        else:
            st.caption("No filters applied — showing all leads after search.")

        # Saved search actions
        if delete_clicked and saved_choice != "— Select —":
            sid = saved_options.get(saved_choice)
            if sid:
                try:
                    delete_saved_search(sid)
                    st.success("Deleted.")
                    st.rerun()
                except Exception as e:
                    st.error(str(e))

        if load_clicked and saved_choice != "— Select —":
            try:
                sid = saved_options.get(saved_choice)
                if sid:
                    rec = get_saved_search(sid)
                    if rec:
                        fd = json.loads(rec["filters_json"])
                        query  = "SELECT * FROM properties WHERE 1=1"
                        params = []
                        sel_state = fd.get("selected_state", "All States")
                        if sel_state != "All States":
                            query += f" AND {STATE_COL} = %s"; params.append(sel_state)
                        if fd.get("prop_types"):
                            query += " AND property_type = ANY(%s)"; params.append(fd["prop_types"])
                        if fd.get("bed_value"):
                            query += " AND beds >= %s"; params.append(fd["bed_value"])
                        if fd.get("bath_value"):
                            query += " AND baths >= %s"; params.append(fd["bath_value"])
                        query += " ORDER BY street_address"
                        conn = get_db_connection()
                        df   = pd.read_sql(query, conn, params=params)
                        conn.close()
                        st.session_state["search_results"] = df
                        st.session_state["search_params"]  = {"show_only_multi": False, "min_appearances": 2}
                        st.success(f"Loaded «{rec['name']}» — {len(df):,} leads.")
                        st.rerun()
            except Exception as e:
                st.error(str(e))

        if save_clicked and save_name and save_name.strip():
            fd = dict(selected_state=selected_state, prop_types=prop_types, bed_value=bed_value,
                      bath_value=bath_value, occupancy_list=occupancy_list, apn_search=apn_search or "",
                      owner_name_contains=locals().get("owner_name_contains",""), owner_types=owner_types,
                      is_absentee=is_absentee, years_owned_min=years_owned_min, tax_year_max=tax_year_max,
                      est_value_min=est_value_min, est_value_max=est_value_max,
                      est_equity_min=est_equity_min, est_equity_max=est_equity_max,
                      est_equity_pct_min=est_equity_pct_min, est_equity_pct_max=est_equity_pct_max,
                      assessed_min=0, assessed_max=0,
                      last_sale_min=last_sale_min, last_sale_max=last_sale_max,
                      filter_by_sale_date=filter_by_sale_date,
                      last_sale_date=last_sale_date.isoformat() if last_sale_date else None,
                      private_loan=private_loan, cash_buyer=cash_buyer,
                      show_only_multi=show_only_multi, min_appearances=min_appearances)
            try:
                save_saved_search(save_name.strip(), json.dumps(fd))
                st.success(f"Saved «{save_name.strip()}».")
                st.rerun()
            except Exception as e:
                st.error(str(e))

        # ── Build & run query ──
        if run_search:
            st.session_state.pop("loaded_search_name", None)
            if show_only_multi:
                query  = """
                SELECT p.*, addr.appearance_count
                FROM properties p
                INNER JOIN (
                    SELECT LOWER(TRIM(street_address)) as normalized_addr, COUNT(*) as appearance_count
                    FROM properties
                    WHERE street_address IS NOT NULL AND TRIM(street_address) != ''
                    GROUP BY LOWER(TRIM(street_address))
                    HAVING COUNT(*) >= %s
                ) addr ON LOWER(TRIM(p.street_address)) = addr.normalized_addr
                WHERE 1=1"""
                params = [int(min_appearances)]
            else:
                query  = "SELECT * FROM properties WHERE 1=1"
                params = []

            if selected_state != "All States":
                prefix = "p." if show_only_multi else ""
                if "state" in _cols and "property_state" in _cols:
                    query += f" AND ({prefix}state = %s OR {prefix}property_state = %s)"
                    params += [selected_state, selected_state]
                else:
                    query += f" AND {prefix}{STATE_COL} = %s"; params.append(selected_state)
            if prop_types:     query += " AND property_type = ANY(%s)"; params.append(prop_types)
            if bed_value:      query += " AND beds >= %s";              params.append(bed_value)
            if bath_value:     query += " AND baths >= %s";             params.append(bath_value)
            if occupancy_list: query += " AND occupancy_status = ANY(%s)"; params.append(occupancy_list)
            if apn_search:     query += " AND apn ILIKE %s";            params.append(f"%{apn_search}%")
            if locals().get("owner_name_contains"):
                query += " AND owner_name ILIKE %s"; params.append(f"%{owner_name_contains}%")
            if owner_types:    query += " AND owner_type = ANY(%s)";    params.append(owner_types)
            if is_absentee:    query += " AND is_absentee = TRUE"
            if years_owned_min > 0: query += " AND years_owned >= %s"; params.append(years_owned_min)
            if tax_year_max < datetime.datetime.now().year:
                query += " AND tax_delinquent_year <= %s"; params.append(tax_year_max)
            if est_value_min > 0:  query += " AND est_value >= %s";     params.append(est_value_min)
            if est_value_max > 0:  query += " AND est_value <= %s";     params.append(est_value_max)
            if est_equity_min > 0: query += " AND est_equity_amt >= %s"; params.append(est_equity_min)
            if est_equity_max > 0: query += " AND est_equity_amt <= %s"; params.append(est_equity_max)
            if est_equity_pct_min > 0:  query += " AND est_equity_pct >= %s"; params.append(est_equity_pct_min)
            if est_equity_pct_max < 100: query += " AND est_equity_pct <= %s"; params.append(est_equity_pct_max)
            if last_sale_min > 0: query += " AND last_sale_price >= %s"; params.append(last_sale_min)
            if last_sale_max > 0: query += " AND last_sale_price <= %s"; params.append(last_sale_max)
            if filter_by_sale_date and last_sale_date:
                query += " AND last_sale_date <= %s"; params.append(last_sale_date)
            if private_loan:  query += " AND has_private_loan = TRUE"
            if cash_buyer:    query += " AND is_cash_buyer = TRUE"
            if filter_by_distress and min_distress > 1:
                query += " AND motivation_score >= %s"; params.append(min_distress)
            query += " ORDER BY motivation_score DESC NULLS LAST, street_address" if not show_only_multi else " ORDER BY addr.appearance_count DESC, street_address"
            try:
                conn = get_db_connection()
                df   = pd.read_sql(query, conn, params=params)
                conn.close()
                st.session_state["search_results"] = df
                st.session_state["search_params"]  = {"show_only_multi": show_only_multi, "min_appearances": min_appearances}
                st.session_state["last_query"]     = query
                st.session_state["last_params"]    = params
                st.rerun()
            except Exception as e:
                st.error(f"Query error: {e}")
                st.exception(e)

        # ── Display results ──
        if "search_results" in st.session_state:
            df = st.session_state["search_results"]
            sp = st.session_state.get("search_params", {})
            sm = sp.get("show_only_multi", False)
            ma = sp.get("min_appearances", 2)

            # ── VIEW KPI CARDS ──
            lead_ids = df["id"].tolist() if "id" in df.columns else []
            if lead_ids:
                try:
                    kpis = get_view_kpis(lead_ids[:500])
                    vk1, vk2, vk3, vk4, vk5 = st.columns(5)
                    vk1.markdown(f'''<div class="metric-tile">
                        <div class="label">Leads Found</div>
                        <div class="value">{len(df):,}</div>
                    </div>''', unsafe_allow_html=True)
                    vk2.markdown(f'''<div class="metric-tile">
                        <div class="label">Total Equity in View</div>
                        <div class="value">${kpis["total_equity"]/1_000_000:.1f}M</div>
                    </div>''', unsafe_allow_html=True)
                    vk3.markdown(f'''<div class="metric-tile">
                        <div class="label">Avg Motivation Score</div>
                        <div class="value">{kpis["avg_score"]}/10</div>
                    </div>''', unsafe_allow_html=True)
                    vk4.markdown(f'''<div class="metric-tile">
                        <div class="label">Vacant</div>
                        <div class="value">{kpis["vacant_count"]:,}</div>
                    </div>''', unsafe_allow_html=True)
                    vk5.markdown(f'''<div class="metric-tile">
                        <div class="label">Absentee</div>
                        <div class="value">{kpis["absentee_count"]:,}</div>
                    </div>''', unsafe_allow_html=True)
                    st.markdown("<div style='margin-bottom:1rem;'></div>", unsafe_allow_html=True)
                except Exception:
                    pass

            # ── MAP VIEW (split: map top, table bottom) ──
            map_tab, table_tab, stack_tab = st.tabs(["🗺 Map View", "📋 Table View", "🔥 Stacked Leads"])

            with map_tab:
                try:
                    import pydeck as pdk
                    map_df = df.copy()
                    # Use lat/lon if present, otherwise try to render what we have
                    has_coords = "lat" in map_df.columns and "lon" in map_df.columns
                    if has_coords:
                        map_df = map_df[map_df["lat"].notna() & map_df["lon"].notna()]

                    if has_coords and len(map_df) > 0:
                        # Color by motivation score
                        def score_color(score):
                            if score is None or score == 0: return [100, 149, 237, 180]
                            if score >= 8: return [248, 81, 73, 220]
                            if score >= 6: return [251, 143, 68, 220]
                            if score >= 4: return [227, 179, 65, 200]
                            return [63, 185, 80, 180]
                        map_df["color"] = map_df["motivation_score"].apply(score_color)
                        map_df["tooltip_text"] = map_df.apply(
                            lambda r: f"{r.get('street_address','')} | Score: {r.get('motivation_score','?')}", axis=1
                        )

                        layer = pdk.Layer(
                            "ScatterplotLayer",
                            data=map_df,
                            get_position=["lon","lat"],
                            get_color="color",
                            get_radius=60,
                            radius_min_pixels=5,
                            radius_max_pixels=20,
                            pickable=True,
                            auto_highlight=True,
                        )
                        view = pdk.ViewState(
                            latitude=map_df["lat"].mean(),
                            longitude=map_df["lon"].mean(),
                            zoom=10,
                            pitch=0,
                        )
                        st.pydeck_chart(pdk.Deck(
                            layers=[layer],
                            initial_view_state=view,
                            map_style="mapbox://styles/mapbox/dark-v10",
                            tooltip={"text": "{tooltip_text}"},
                        ))
                        st.caption(f"🔴 High (8-10)  🟠 Med-High (6-7)  🟡 Med (4-5)  🟢 Low  · {len(map_df):,} pins")
                    else:
                        st.info("📍 No geocoded leads yet. Click **Geocode Results** to add map pins.")
                        gc1, gc2 = st.columns([2,1])
                        with gc1:
                            st.caption("Geocoding uses OpenStreetMap (free). ~1 sec per lead.")
                        with gc2:
                            if st.button("🌐 Geocode Results", type="primary", key="geocode_btn"):
                                with st.spinner("Geocoding..."):
                                    try:
                                        ensure_lat_lon_columns()
                                        ids_to_geo = lead_ids[:50]
                                        done = 0
                                        for lid in ids_to_geo:
                                            from core import geocode_lead
                                            if geocode_lead(lid): done += 1
                                        st.success(f"Geocoded {done} leads. Refresh search to see pins.")
                                        st.rerun()
                                    except Exception as ge:
                                        st.error(str(ge))
                except ImportError:
                    st.warning("pydeck not installed. Add `pydeck` to requirements.txt")
                except Exception as me:
                    st.error(f"Map error: {me}")

            with stack_tab:
                st.markdown('<div class="section-title">🔥 List Stacking — High Priority Leads</div>', unsafe_allow_html=True)
                st.caption("Leads appearing on 2+ of your imported lists are the hottest — motivated sellers on multiple radars.")
                min_stack = st.slider("Minimum list appearances", 2, 10, 2, key="stack_slider")
                try:
                    stacked = get_stacked_leads(min_stack)
                    summary = get_list_stack_summary()
                    if summary:
                        sc = st.columns(len(summary))
                        for i, s in enumerate(summary):
                            sc[i].markdown(f'''<div class="metric-tile">
                                <div class="label">{s["list_count"]} Lists</div>
                                <div class="value" style="color:#f85149;">{s["lead_count"]:,}</div>
                                <div class="delta">stacked leads</div>
                            </div>''', unsafe_allow_html=True)
                        st.markdown("<div style='margin-top:1rem;'></div>", unsafe_allow_html=True)
                    if stacked:
                        stack_df = pd.DataFrame(stacked)
                        st.dataframe(stack_df, use_container_width=True, hide_index=True)
                        csv = stack_df.to_csv(index=False)
                        st.download_button("📥 Export Stacked Leads", csv,
                            f"stacked_leads_{datetime.datetime.now().strftime('%Y%m%d')}.csv", "text/csv")
                    else:
                        st.info("No stacked leads yet. Import multiple lists with the same addresses to see overlaps here.")
                except Exception as se:
                    st.error(f"Stacking error: {se}")

            with table_tab:
                st.markdown(f'<div class="section-title">🏠 {len(df):,} Leads Found</div>', unsafe_allow_html=True)

            # Batch toolbar — shown below tabs
            st.markdown("<div style='margin-top:0.5rem;'></div>", unsafe_allow_html=True)
            b1, b2, b3, b4, b5, b6 = st.columns([1,1,1,1,1,2])
            with b1:
                if st.button("☑ All",    key="btn_sel_all"):   st.session_state["select_all_leads"] = True;  st.rerun()
            with b2:
                if st.button("✕ Clear",  key="btn_clr_all"):   st.session_state["select_all_leads"] = False; st.rerun()
            with b3:
                if st.button("✏️ Update", key="batch_update"):  st.session_state["batch_action"] = "update"
            with b4:
                if st.button("🏷 Tag",    key="batch_tag"):     st.session_state["batch_action"] = "tag"
            with b5:
                if st.button("📤 Export", key="batch_export"):  st.session_state["batch_action"] = "export"
            with b6:
                if st.button("🗑 Delete", key="batch_delete"):  st.session_state["batch_action"] = "delete"

            # Hide empty columns
            show_all = st.checkbox("Show all columns", value=False, key="show_all_cols")
            disp_df  = df.copy()
            if not show_all:
                always   = {"id","street_address","city","owner_name"}
                cond     = {"state","property_state","phone_numbers","tags","stage","motivation_score","zip_code","apn","property_type","beds","baths"}
                def has_data(s):
                    nn = s.notna()
                    if not nn.any(): return 0
                    if s.dtype == "object":
                        bad = {"none","nan","","null","<na>","na"}
                        return (nn & (~s.astype(str).str.strip().str.lower().isin(bad))).sum()
                    return nn.sum()
                keep = [c for c in disp_df.columns if
                        c in always or
                        (c in cond and has_data(disp_df[c]) > len(disp_df)*0.3) or
                        (c not in always and c not in cond and has_data(disp_df[c]) > len(disp_df)*0.1)]
                disp_df = disp_df[[c for c in disp_df.columns if c in keep]]

            df_sel = disp_df.copy()
            if "selected" not in df_sel.columns:
                df_sel.insert(0, "selected", False)
            if st.session_state.get("select_all_leads") is True:
                df_sel["selected"] = True
            elif st.session_state.get("select_all_leads") is False:
                df_sel["selected"] = False
                st.session_state.pop("select_all_leads", None)

            edited = st.data_editor(
                df_sel, use_container_width=True, hide_index=True,
                column_config={"selected": st.column_config.CheckboxColumn("✓", default=False)},
                disabled=[c for c in df_sel.columns if c != "selected"],
                key="lead_editor",
            )

            # ── Batch actions ──
            if "batch_action" in st.session_state:
                selected_rows = edited[edited["selected"] == True]
                if len(selected_rows) == 0:
                    st.warning("Select at least one lead.")
                    del st.session_state["batch_action"]
                    st.rerun()
                else:
                    act = st.session_state["batch_action"]

                    if act == "export":
                        csv = selected_rows.drop(columns=["selected"]).to_csv(index=False)
                        st.download_button("📥 Download CSV", csv,
                                           f"leads_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.csv", "text/csv")
                        del st.session_state["batch_action"]

                    elif act == "delete":
                        st.error(f"Delete {len(selected_rows)} leads? This cannot be undone.")
                        d1, d2 = st.columns(2)
                        with d1:
                            if st.button("✅ Yes, Delete"):
                                ids = tuple(selected_rows["id"].tolist())
                                ids_str = f"({ids[0]})" if len(ids)==1 else str(ids)
                                execute_query(f"DELETE FROM properties WHERE id IN {ids_str}")
                                st.success(f"Deleted {len(selected_rows)} leads.")
                                del st.session_state["batch_action"]
                                del st.session_state["search_results"]
                                st.rerun()
                        with d2:
                            if st.button("❌ Cancel", key="del_cancel"):
                                del st.session_state["batch_action"]; st.rerun()

                    elif act == "update":
                        st.markdown('<div class="section-title">Batch Update</div>', unsafe_allow_html=True)
                        with st.form("batch_update_form"):
                            new_motivation = st.number_input("Motivation Score (0 = skip)", 0, 10, 0)
                            new_stage      = st.selectbox("Stage", ["", "New","Contacted","Negotiating","Closed","Lost"])
                            new_notes      = st.text_area("Append notes")
                            s1, s2 = st.columns(2)
                            with s1: submitted = st.form_submit_button("Apply", type="primary")
                            with s2: cancelled = st.form_submit_button("Cancel")
                            if submitted:
                                ids = list(selected_rows["id"])
                                parts, pms = [], []
                                if new_motivation > 0:  parts.append("motivation_score = %s"); pms.append(new_motivation)
                                if new_stage:           parts.append("stage = %s");            pms.append(new_stage)
                                if new_notes:           parts.append("notes = CONCAT(COALESCE(notes,''), %s, '\n')"); pms.append(new_notes)
                                if parts:
                                    ph = ",".join(["%s"]*len(ids))
                                    execute_query(f"UPDATE properties SET {', '.join(parts)} WHERE id IN ({ph})", pms+ids)
                                    st.success(f"Updated {len(ids)} leads.")
                                    del st.session_state["batch_action"]
                                    del st.session_state["search_results"]
                                    st.rerun()
                            if cancelled:
                                del st.session_state["batch_action"]; st.rerun()

                    elif act == "tag":
                        st.markdown('<div class="section-title">Manage Tags</div>', unsafe_allow_html=True)
                        full_df = st.session_state["search_results"]
                        sel_ids = selected_rows["id"].tolist()
                        id_to_tags = (full_df[full_df["id"].isin(sel_ids)][["id","tags"]].set_index("id")["tags"]
                                      if "tags" in full_df.columns else pd.Series(dtype=object))
                        all_t = set()
                        for tgs in id_to_tags.dropna():
                            all_t.update([t.strip() for t in str(tgs).split(",") if t.strip()])
                        st.caption("Current tags: " + (", ".join(sorted(all_t)) if all_t else "None"))
                        with st.form("batch_tag_form"):
                            action     = st.radio("Action", ["Add Tags","Remove Tags"], horizontal=True)
                            tags_input = st.text_input("Tags (comma-separated)")
                            t1, t2 = st.columns(2)
                            with t1: ts = st.form_submit_button("Apply", type="primary")
                            with t2: tc = st.form_submit_button("Cancel")
                            if ts and tags_input:
                                tag_list = [t.strip() for t in tags_input.split(",") if t.strip()]
                                for lid in sel_ids:
                                    cur = id_to_tags.get(lid) if lid in id_to_tags.index else None
                                    cur_set = set([t.strip() for t in str(cur).split(",") if t.strip()]) if pd.notna(cur) and cur else set()
                                    if action == "Add Tags":    cur_set.update(tag_list)
                                    else:                       cur_set -= set(tag_list)
                                    execute_query("UPDATE properties SET tags = %s WHERE id = %s",
                                                  (", ".join(sorted(cur_set)) or None, lid))
                                st.success(f"Tags updated for {len(sel_ids)} leads.")
                                del st.session_state["batch_action"]
                                del st.session_state["search_results"]
                                st.rerun()
                            if tc:
                                del st.session_state["batch_action"]; st.rerun()

                    elif act == "skip_trace":
                        st.markdown('<div class="section-title">🔍 Skip Trace Selected Leads</div>', unsafe_allow_html=True)
                        sel_ids = selected_rows["id"].tolist()
                        st.info(f"Skip trace {len(sel_ids)} lead(s) to unlock phone numbers and emails.")
                        st.caption("Configure your API key in `.streamlit/secrets.toml` under `[skip_trace]`.")

                        provider_choice = st.selectbox(
                            "Provider",
                            ["batch_skip_tracing", "skip_genie"],
                            format_func=lambda x: {
                                "batch_skip_tracing": "BatchSkipTracing.com",
                                "skip_genie": "SkipGenie.com",
                            }[x],
                            key="skip_provider"
                        )
                        sk1, sk2 = st.columns(2)
                        with sk1:
                            if st.button("🔍 Run Skip Trace", type="primary", key="skip_run"):
                                results_sk = {"success": 0, "failed": 0, "errors": []}
                                prog_sk = st.progress(0)
                                for i, lid in enumerate(sel_ids):
                                    r = skip_trace_lead(int(lid), provider_choice)
                                    if r["success"]:
                                        results_sk["success"] += 1
                                    else:
                                        results_sk["failed"] += 1
                                        if r.get("error"):
                                            results_sk["errors"].append(f"#{lid}: {r['error']}")
                                    prog_sk.progress((i+1)/len(sel_ids))
                                if results_sk["success"] > 0:
                                    st.success(f"✅ Skip traced {results_sk['success']} leads successfully.")
                                if results_sk["failed"] > 0:
                                    st.warning(f"⚠️ {results_sk['failed']} failed.")
                                if results_sk["errors"]:
                                    with st.expander("Errors"):
                                        for e in results_sk["errors"][:10]:
                                            st.caption(e)
                                del st.session_state["batch_action"]
                                del st.session_state["search_results"]
                                st.rerun()
                        with sk2:
                            if st.button("Cancel", key="skip_cancel"):
                                del st.session_state["batch_action"]; st.rerun()

            # Notes
            if len(df) > 0 and "id" in df.columns:
                with st.expander("📝 Notes & Activity for a Lead"):
                    choices = {f"{r.get('id')} — {r.get('street_address','?')}, {r.get('city','?')}, {r.get('state') or r.get('property_state','?')}": r.get("id")
                               for _, r in df.head(300).iterrows()}
                    picked = st.selectbox("Select lead", [""] + list(choices.keys()), key="notes_lead_search")
                    if picked and picked in choices:
                        lid = choices[picked]
                        acts = get_lead_activities(lid)
                        for a in acts:
                            st.markdown(f"**{a.get('activity_type','note')}** — {a.get('created_at')}")
                            if a.get("content"): st.text(a["content"])
                            st.caption("---")
                        if not acts: st.info("No activity yet.")
                        with st.form("add_act_form", clear_on_submit=True):
                            at = st.selectbox("Type", ["note","call","email","meeting","status_change"])
                            ac = st.text_area("Content")
                            if st.form_submit_button("Add"):
                                if ac.strip(): add_lead_activity(lid, at, ac.strip()); st.success("Added."); st.rerun()
                                else: st.warning("Enter some content.")
        else:
            st.markdown("""
            <div class="re-card" style="text-align:center;padding:3rem 2rem;">
                <div style="font-size:2.5rem;margin-bottom:1rem;">🔍</div>
                <div style="font-size:1rem;font-weight:600;color:#e6edf3;">Set your filters and click Run Search</div>
                <div style="font-size:0.85rem;margin-top:0.5rem;color:#8b949e;">Results will appear here with batch actions for export, tagging, and pipeline updates.</div>
            </div>""", unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════
# PAGE: PIPELINE
# ═══════════════════════════════════════════════════════════════
elif page_key == "Pipeline":
    st.markdown('<div class="page-title">Pipeline</div>', unsafe_allow_html=True)
    st.markdown('<div class="page-sub">Manage your deals by stage. Select leads and move them through the funnel.</div>', unsafe_allow_html=True)

    if "stage" not in _cols:
        st.warning("No `stage` column found. Run: `ALTER TABLE properties ADD COLUMN stage VARCHAR(50);`")
    else:
        try:
            counts    = get_pipeline_counts()
            stage_map = {r["stage"]: r["cnt"] for r in counts} if counts else {}
            PIPELINE_STAGES = ["New","Contacted","Negotiating","Closed","Lost","Unset"]
            STAGE_CSS = {"New":"stage-new","Contacted":"stage-contacted","Negotiating":"stage-negotiating",
                         "Closed":"stage-closed","Lost":"stage-lost","Unset":"stage-unset"}

            # ── Board ──
            st.markdown('<div class="section-title">Deal Board</div>', unsafe_allow_html=True)
            board_cols = st.columns(len(PIPELINE_STAGES))
            for i, s in enumerate(PIPELINE_STAGES):
                cnt = stage_map.get(s, 0)
                css = STAGE_CSS.get(s, "stage-unset")
                board_cols[i].markdown(f"""
                <div class="pipeline-col">
                    <div class="pipeline-header {css}">{s}</div>
                    <div class="pipeline-count">{cnt:,}</div>
                    <div class="pipeline-sub">leads</div>
                </div>""", unsafe_allow_html=True)

            st.markdown("<div style='margin-top:1.5rem;'></div>", unsafe_allow_html=True)

            # ── Funnel totals ──
            total_pl = sum(stage_map.values())
            if total_pl > 0:
                prog_cols = st.columns(len(PIPELINE_STAGES))
                for i, s in enumerate(PIPELINE_STAGES):
                    cnt = stage_map.get(s, 0)
                    pct = (cnt / total_pl * 100) if total_pl else 0
                    prog_cols[i].progress(int(pct), text=f"{pct:.0f}%")

            st.divider()

            # ── Move leads ──
            st.markdown('<div class="section-title">Move Leads</div>', unsafe_allow_html=True)
            mc1, mc2, mc3 = st.columns([2, 2, 1])
            with mc1:
                stage_filter = st.selectbox("Show stage", ["All"] + [s for s in PIPELINE_STAGES if stage_map.get(s, 0) > 0 or s == "Unset"])
            with mc2:
                new_stage = st.selectbox("Move selected to", ["New","Contacted","Negotiating","Closed","Lost"])
            with mc3:
                st.markdown("<div style='margin-top:1.6rem;'></div>", unsafe_allow_html=True)
                move_btn = st.button("Apply ▶", type="primary", use_container_width=True)

            leads_raw = get_leads_by_stage(stage_filter)
            if not leads_raw:
                st.info("No leads in this stage.")
            else:
                df_pl = pd.DataFrame(leads_raw)
                st.caption(f"{len(df_pl):,} leads — check rows then click Apply to move them.")

                df_pl_sel = df_pl.copy()
                if "selected" not in df_pl_sel.columns:
                    df_pl_sel.insert(0, "selected", False)

                # Show only useful columns
                show_cols = ["selected"] + [c for c in ["id","street_address","city","state","owner_name","stage","motivation_score","tags"] if c in df_pl_sel.columns]
                edited_pl = st.data_editor(
                    df_pl_sel[show_cols], use_container_width=True, hide_index=True,
                    column_config={"selected": st.column_config.CheckboxColumn("✓", default=False)},
                    disabled=[c for c in show_cols if c != "selected"],
                    key="pipeline_editor",
                )
                if move_btn:
                    sel = edited_pl[edited_pl["selected"] == True]
                    if len(sel) == 0:
                        st.warning("Select at least one lead.")
                    else:
                        update_stage(list(sel["id"]), new_stage)
                        st.success(f"Moved {len(sel)} lead(s) → **{new_stage}**.")
                        st.rerun()

            st.divider()

            # ── Notes ──
            st.markdown('<div class="section-title">Notes & Activity</div>', unsafe_allow_html=True)
            lead_options = get_leads_by_stage(None)
            if lead_options:
                choices_pl = {f"{r.get('id')} — {r.get('street_address','?')}, {r.get('city','?')}": r["id"]
                              for r in lead_options[:500]}
                picked_pl = st.selectbox("Select lead", [""] + list(choices_pl.keys()), key="notes_lead_pipeline")
                if picked_pl and picked_pl in choices_pl:
                    lid_pl = choices_pl[picked_pl]
                    acts   = get_lead_activities(lid_pl)
                    for a in acts:
                        st.markdown(f"**{a.get('activity_type','note')}** — {a.get('created_at')}")
                        if a.get("content"): st.text(a["content"])
                        st.caption("---")
                    if not acts: st.info("No activity yet.")
                    with st.form("add_activity_pipeline", clear_on_submit=True):
                        at2 = st.selectbox("Type", ["note","call","email","meeting","status_change"])
                        ac2 = st.text_area("Content")
                        if st.form_submit_button("Add", type="primary"):
                            if ac2.strip(): add_lead_activity(lid_pl, at2, ac2.strip()); st.success("Added."); st.rerun()
                            else: st.warning("Enter some content.")
        except Exception as e:
            st.error(str(e)); st.exception(e)


# ═══════════════════════════════════════════════════════════════
# PAGE: IMPORT
# ═══════════════════════════════════════════════════════════════
elif page_key == "Import":
    st.markdown('<div class="page-title">📥 Import</div>', unsafe_allow_html=True)
    st.markdown('<div class="page-sub">Upload CSV lists, add phone numbers, or enter leads manually.</div>', unsafe_allow_html=True)

    import_tab1, import_tab2, import_tab3 = st.tabs(["📂 CSV Lead Import", "📞 Add Phone Numbers", "✏️ Manual Lead Entry"])

    # ═══════════════════════════════════════════════════════════════
    # TAB 1 — CSV BULK IMPORT
    # ═══════════════════════════════════════════════════════════════
    with import_tab1:
        with st.expander("ℹ️ How it works", expanded=False):
            st.markdown("""
            <div class="step-row"><div class="step-badge">1</div> Upload one or more CSV files below.</div>
            <div class="step-row"><div class="step-badge">2</div> Columns are auto-matched by header name — adjust if needed.</div>
            <div class="step-row"><div class="step-badge">3</div> Click <strong>Import</strong>. Duplicate addresses bump the motivation score; new addresses are inserted.</div>
            <div class="step-row"><div class="step-badge">4</div> Results appear in <strong>Lead Engine</strong> immediately.</div>
            """, unsafe_allow_html=True)

        ic1, ic2 = st.columns([2, 1])
        with ic1:
            source_name = st.text_input("List source name", placeholder="e.g. Foreclosure list — OH — March 2025", key="bulk_source")
        with ic2:
            st.markdown("<div style='margin-top:1.75rem;'></div>", unsafe_allow_html=True)
            uploaded_files = st.file_uploader("Upload CSV(s)", type=["csv"], accept_multiple_files=True, key="bulk_uploader", label_visibility="collapsed")

        if not uploaded_files:
            st.markdown("""
            <div class="re-card" style="text-align:center;padding:2.5rem;">
                <div style="font-size:2rem;margin-bottom:0.75rem;">📂</div>
                <div style="font-weight:600;color:#e6edf3;">Drop your CSV files above to get started</div>
                <div style="font-size:0.82rem;margin-top:0.4rem;color:#8b949e;">Accepts .csv · Multiple files supported · Up to 500 rows per batch</div>
            </div>""", unsafe_allow_html=True)
        else:
            st.success(f"✅ {len(uploaded_files)} file(s) ready")
            file_tabs = st.tabs([f"📄 {f.name}" for f in uploaded_files])

            for idx, (tab, uploaded_file) in enumerate(zip(file_tabs, uploaded_files)):
                with tab:
                    try:
                        try:    raw_df = pd.read_csv(uploaded_file, encoding="utf-8")
                        except: raw_df = pd.read_csv(uploaded_file, encoding="latin-1")
                        raw_df.columns = [str(c).strip() for c in raw_df.columns]

                        with st.expander("👁 Preview first 10 rows"):
                            st.dataframe(raw_df.head(10), use_container_width=True)
                            st.caption(f"{len(raw_df):,} rows · {len(raw_df.columns)} columns")

                        csv_cols = list(raw_df.columns)
                        cols     = ["None"] + csv_cols
                        di       = _default_indices(csv_cols)
                        def _idx(k): return min(di.get(k, 0), len(cols)-1)

                        st.markdown('<div class="section-title" style="margin-top:1rem;">Column Mapping</div>', unsafe_allow_html=True)
                        st.caption("Auto-matched from your CSV headers. Adjust any dropdown if a field mapped incorrectly.")

                        st.markdown("**📍 Property Location** *(required)*")
                        e1,e2,e3,e4,e5 = st.columns(5)
                        with e1: m_prop_addr  = st.selectbox("Address *",  cols, _idx("property_address"), key=f"prop_addr_{idx}")
                        with e2: m_prop_city  = st.selectbox("City *",     cols, _idx("property_city"),    key=f"prop_city_{idx}")
                        with e3: m_prop_state = st.selectbox("State *",    cols, _idx("property_state"),   key=f"prop_state_{idx}")
                        with e4: m_prop_zip   = st.selectbox("Zip",        cols, _idx("property_zip"),     key=f"prop_zip_{idx}")
                        with e5: m_prop_county= st.selectbox("County",     cols, _idx("property_county"),  key=f"prop_county_{idx}")

                        st.markdown("**👤 Owner Name** *(at least one required)*")
                        n1,n2 = st.columns(2)
                        with n1: m_first = st.selectbox("First Name *", cols, _idx("first_name"), key=f"first_name_{idx}")
                        with n2: m_last  = st.selectbox("Last Name *",  cols, _idx("last_name"),  key=f"last_name_{idx}")

                        with st.expander("👥 Additional Owners (2–4)"):
                            a1,a2 = st.columns(2)
                            with a1: m_o2f = st.selectbox("Owner 2 First", cols, _idx("owner_2_first_name"), key=f"o2f_{idx}")
                            with a2: m_o2l = st.selectbox("Owner 2 Last",  cols, _idx("owner_2_last_name"),  key=f"o2l_{idx}")
                            a3,a4 = st.columns(2)
                            with a3: m_o3f = st.selectbox("Owner 3 First", cols, _idx("owner_3_first_name"), key=f"o3f_{idx}")
                            with a4: m_o3l = st.selectbox("Owner 3 Last",  cols, _idx("owner_3_last_name"),  key=f"o3l_{idx}")
                            a5,a6 = st.columns(2)
                            with a5: m_o4f = st.selectbox("Owner 4 First", cols, _idx("owner_4_first_name"), key=f"o4f_{idx}")
                            with a6: m_o4l = st.selectbox("Owner 4 Last",  cols, _idx("owner_4_last_name"),  key=f"o4l_{idx}")

                        st.markdown("**📬 Mailing Address**")
                        m1,m2,m3,m4 = st.columns(4)
                        with m1: m_mail_addr  = st.selectbox("Mailing Address", cols, _idx("mailing_address"), key=f"mail_addr_{idx}")
                        with m2: m_mail_city  = st.selectbox("Mailing City",    cols, _idx("mailing_city"),    key=f"mail_city_{idx}")
                        with m3: m_mail_state = st.selectbox("Mailing State",   cols, _idx("mailing_state"),   key=f"mail_sta_{idx}")
                        with m4: m_mail_zip   = st.selectbox("Mailing Zip",     cols, _idx("mailing_zip"),     key=f"mail_zip_{idx}")

                        st.markdown("**📞 Phone Numbers**")
                        p1,p2,p3,p4 = st.columns(4)
                        with p1: m_ph1 = st.selectbox("Phone 1", cols, _idx("phone_1"), key=f"ph1_{idx}")
                        with p2: m_ph2 = st.selectbox("Phone 2", cols, _idx("phone_2"), key=f"ph2_{idx}")
                        with p3: m_ph3 = st.selectbox("Phone 3", cols, _idx("phone_3"), key=f"ph3_{idx}")
                        with p4: m_ph4 = st.selectbox("Phone 4", cols, _idx("phone_4"), key=f"ph4_{idx}")

                        with st.expander("📊 Optional Fields (Property Details, Financials, etc.)"):
                            st.markdown("**Property Details**")
                            o1,o2,o3,o4 = st.columns(4)
                            with o1: m_apn       = st.selectbox("APN",           cols, _idx("apn"),           key=f"apn_{idx}")
                            with o2: m_prop_type = st.selectbox("Property Type", cols, _idx("property_type"), key=f"prop_type_{idx}")
                            with o3: m_prop_use  = st.selectbox("Property Use",  cols, _idx("property_use"),  key=f"prop_use_{idx}")
                            with o4: m_land_use  = st.selectbox("Land Use",      cols, _idx("land_use"),      key=f"land_use_{idx}")

                            st.markdown("**Size & Structure**")
                            s1,s2,s3,s4 = st.columns(4)
                            with s1: m_sqft    = st.selectbox("Living SqFt",  cols, _idx("living_sqft"), key=f"sqft_{idx}")
                            with s2: m_acres   = st.selectbox("Lot Acres",    cols, _idx("lot_acres"),   key=f"acres_{idx}")
                            with s3: m_yr_blt  = st.selectbox("Year Built",   cols, _idx("year_built"),  key=f"yr_blt_{idx}")
                            with s4: m_beds    = st.selectbox("Beds",         cols, _idx("beds"),        key=f"beds_{idx}")
                            s5,s6,s7,s8 = st.columns(4)
                            with s5: m_baths   = st.selectbox("Baths",        cols, _idx("baths"),       key=f"baths_{idx}")
                            with s6: m_story   = st.selectbox("Stories",      cols, _idx("stories"),     key=f"stories_{idx}")
                            with s7: m_units   = st.selectbox("Units",        cols, _idx("units_count"), key=f"units_{idx}")
                            with s8: m_lotsqft = st.selectbox("Lot SqFt",     cols, _idx("lot_sqft"),    key=f"lotsqft_{idx}")

                            st.markdown("**Financial**")
                            f1,f2,f3 = st.columns(3)
                            with f1: m_val   = st.selectbox("Est Value",       cols, _idx("est_value"),       key=f"val_{idx}")
                            with f2: m_sale  = st.selectbox("Last Sale Price", cols, _idx("last_sale_price"), key=f"sale_{idx}")
                            with f3: m_occ   = st.selectbox("Occupancy",       cols, _idx("occupancy"),       key=f"occ_{idx}")

                            st.markdown("**Owner Info**")
                            oi1,oi2,oi3,oi4 = st.columns(4)
                            with oi1: m_own_months  = st.selectbox("Ownership Months", cols, _idx("ownership_length_months"), key=f"own_mo_{idx}")
                            with oi2: m_owner_type  = st.selectbox("Owner Type",       cols, _idx("owner_type"),              key=f"own_type_{idx}")
                            with oi3: m_owner_occ   = st.selectbox("Owner Occupied",   cols, _idx("owner_occupied"),          key=f"own_occ_{idx}")
                            with oi4: m_vacant_col  = st.selectbox("Vacant",           cols, _idx("vacant"),                  key=f"vacant_{idx}")

                            st.markdown("**Garage & HVAC**")
                            g1,g2,g3,g4 = st.columns(4)
                            with g1: m_garage_t = st.selectbox("Garage Type",  cols, _idx("garage_type"),  key=f"gar_t_{idx}")
                            with g2: m_garage_s = st.selectbox("Garage SqFt",  cols, _idx("garage_sqft"),  key=f"gar_s_{idx}")
                            with g3: m_ac       = st.selectbox("AC Type",      cols, _idx("ac_type"),      key=f"ac_{idx}")
                            with g4: m_heat     = st.selectbox("Heating Type", cols, _idx("heating_type"), key=f"heat_{idx}")

                        essential_ok = (m_prop_addr != "None" and m_prop_city != "None"
                                        and m_prop_state != "None"
                                        and (m_first != "None" or m_last != "None"))

                        if not essential_ok:
                            st.warning("Map at least: Property Address, City, State, and First or Last Name.")
                        else:
                            st.markdown("<div style='margin-top:0.75rem;'></div>", unsafe_allow_html=True)
                            if st.button(f"⬆️ Import {uploaded_file.name}", key=f"import_{idx}", type="primary", use_container_width=True):
                                prog   = st.progress(0)
                                status = st.empty()
                                stats  = {"new": 0, "error": 0, "skipped": 0}
                                batch  = []
                                BATCH_SIZE = 500

                                for i, row in raw_df.iterrows():
                                    try:
                                        addr_val = row.get(m_prop_addr) if m_prop_addr != "None" else None
                                        if pd.isna(addr_val) or str(addr_val).strip() == "":
                                            stats["skipped"] += 1; continue
                                        city_val  = str(row.get(m_prop_city,"")).strip()[:100]  if m_prop_city  != "None" else ""
                                        state_val = str(row.get(m_prop_state,"")).strip().upper()[:2] if m_prop_state != "None" else ""
                                        if not city_val or not state_val:
                                            stats["skipped"] += 1; continue

                                        payload = {"address": str(addr_val).strip()[:255], "city": city_val, "state": state_val}
                                        if source_name: payload["source"] = source_name

                                        def _s(col, maxlen=255):
                                            if col == "None" or pd.isna(row.get(col)): return None
                                            return str(row[col]).strip()[:maxlen]
                                        def _i(col):
                                            if col == "None" or pd.isna(row.get(col)): return None
                                            try: return int(float(str(row[col]).replace(",","")))
                                            except: return None
                                        def _f(col):
                                            if col == "None" or pd.isna(row.get(col)): return None
                                            try: return float(str(row[col]).replace(",","").replace("$",""))
                                            except: return None
                                        def _b(col, truthy=None):
                                            if col == "None" or pd.isna(row.get(col)): return None
                                            return str(row[col]).strip().lower() in (truthy or ["yes","y","true","1"])

                                        if _s(m_prop_zip, 20):    payload["zip"]    = _s(m_prop_zip, 20)
                                        if _s(m_prop_county, 100): payload["county"] = _s(m_prop_county, 100)

                                        of = _s(m_first, 100); ol = _s(m_last, 100)
                                        if of: payload["owner_first"] = of
                                        if ol: payload["owner_last"]  = ol

                                        parts = []
                                        if of or ol: parts.append(f"{of or ''} {ol or ''}".strip())
                                        for ff, lf in [(m_o2f,m_o2l),(m_o3f,m_o3l),(m_o4f,m_o4l)]:
                                            pf = _s(ff,100); pl = _s(lf,100)
                                            if pf: parts.append(f"{pf} {pl or ''}".strip())
                                        if len(parts) > 1:
                                            payload["owner_name"] = " / ".join(parts)
                                            payload.pop("owner_first",None); payload.pop("owner_last",None)

                                        for key, col, mx in [("mailing_address",m_mail_addr,255),("mailing_city",m_mail_city,100),
                                                              ("mailing_state",m_mail_state,2),("mailing_zip",m_mail_zip,20)]:
                                            v = _s(col, mx)
                                            if v: payload[key] = v

                                        phones = [str(row[ph]).strip() for ph in [m_ph1,m_ph2,m_ph3,m_ph4]
                                                  if ph != "None" and not pd.isna(row.get(ph)) and str(row[ph]).strip()]
                                        if phones: payload["phone_numbers"] = ", ".join(phones)

                                        for key, col in [("apn",m_apn),("property_type",m_prop_type),("property_use",m_prop_use),
                                                         ("land_use",m_land_use),("garage_type",m_garage_t),
                                                         ("ac_type",m_ac),("heating_type",m_heat),("owner_type",m_owner_type)]:
                                            v = _s(col); v and payload.update({key: v})

                                        for key, col in [("living_sqft",m_sqft),("lot_sqft",m_lotsqft),("year_built",m_yr_blt),
                                                         ("stories",m_story),("units_count",m_units),
                                                         ("garage_sqft",m_garage_s),("ownership_length_months",m_own_months)]:
                                            v = _i(col)
                                            if v is not None: payload[key] = v

                                        for key, col in [("lot_acres",m_acres),("baths",m_baths),
                                                         ("est_value",m_val),("last_sale_price",m_sale)]:
                                            v = _f(col)
                                            if v is not None: payload[key] = v

                                        v = _i(m_beds)
                                        if v is not None: payload["beds"] = v

                                        v = _b(m_owner_occ, ["yes","y","true","1","owner occupied"])
                                        if v is not None: payload["owner_occupied"] = v
                                        v = _b(m_vacant_col, ["yes","y","true","1","vacant"])
                                        if v is not None: payload["vacant"] = v

                                        if m_occ != "None" and not pd.isna(row.get(m_occ)):
                                            ov = str(row[m_occ]).strip()
                                            payload["occupancy_status"] = (
                                                "Vacant"   if ov.lower() in ["vacant","v","empty"] else
                                                "Occupied" if ov.lower() in ["occupied","occ","owner occupied"] else ov
                                            )

                                        batch.append(payload)
                                        stats["new"] += 1
                                        if len(batch) >= BATCH_SIZE:
                                            bulk_insert_leads(batch); batch = []
                                    except Exception:
                                        stats["error"] += 1

                                    prog.progress((i+1)/len(raw_df))
                                    status.text(f"Processing {i+1:,} / {len(raw_df):,}")

                                if batch: bulk_insert_leads(batch)

                                st.success(f"✅ Imported **{stats['new']:,}** leads · {stats['error']} errors · {stats['skipped']} skipped")
                                if stats["new"] > 0:
                                    st.balloons()
                                    try:
                                        list_name = source_name.strip() if (source_name and source_name.strip()) else uploaded_file.name
                                        add_uploaded_list(list_name, uploaded_file.name)
                                    except Exception:
                                        pass
                    except Exception as e:
                        st.error(f"Error reading file: {e}")

    # ═══════════════════════════════════════════════════════════════
    # TAB 2 — ADD PHONE NUMBERS
    # ═══════════════════════════════════════════════════════════════
    with import_tab2:
        st.markdown('<div class="section-title">📞 Add / Update Phone Numbers</div>', unsafe_allow_html=True)
        ph_method = st.radio("Method", ["📂 Upload CSV with phones", "✏️ Type phones manually"],
                             horizontal=True, key="ph_method")

        if ph_method == "📂 Upload CSV with phones":
            st.caption("Your CSV needs one column to match leads (address or APN) + phone number columns.")
            phone_file = st.file_uploader("Upload phone CSV", type=["csv"], key="phone_csv_upload")

            if phone_file:
                try:
                    try:    ph_df = pd.read_csv(phone_file, encoding="utf-8")
                    except: ph_df = pd.read_csv(phone_file, encoding="latin-1")
                    ph_df.columns = [str(c).strip() for c in ph_df.columns]

                    st.success(f"Loaded {len(ph_df):,} rows")
                    with st.expander("Preview"):
                        st.dataframe(ph_df.head(8), use_container_width=True)

                    ph_cols = ["None"] + list(ph_df.columns)
                    mc1, mc2 = st.columns(2)
                    with mc1: match_by  = st.selectbox("Match leads by", ["Address", "APN"], key="ph_match_by")
                    with mc2: match_col = st.selectbox("Which column?", ph_cols, key="ph_match_col")

                    p1c,p2c,p3c,p4c = st.columns(4)
                    with p1c: ph1_col = st.selectbox("Phone 1", ph_cols, key="ph1_col")
                    with p2c: ph2_col = st.selectbox("Phone 2", ph_cols, key="ph2_col")
                    with p3c: ph3_col = st.selectbox("Phone 3", ph_cols, key="ph3_col")
                    with p4c: ph4_col = st.selectbox("Phone 4", ph_cols, key="ph4_col")

                    overwrite = st.checkbox("Overwrite existing phones (unchecked = append)", value=False, key="ph_overwrite")

                    if st.button("📥 Import Phone Numbers", type="primary", key="ph_import_btn"):
                        matched = 0; not_found = 0
                        prog_ph = st.progress(0)
                        for i, row in ph_df.iterrows():
                            phones = [str(row[c]).strip() for c in [ph1_col,ph2_col,ph3_col,ph4_col]
                                      if c != "None" and not pd.isna(row.get(c)) and str(row.get(c,"")).strip()
                                      and str(row[c]).strip().lower() not in ("nan","none","null","")]
                            if not phones:
                                prog_ph.progress((i+1)/len(ph_df)); continue
                            try:
                                val = str(row.get(match_col, "")).strip()
                                if not val: continue
                                if match_by == "Address":
                                    ex = execute_query("SELECT id, phone_numbers FROM properties WHERE LOWER(TRIM(street_address)) = LOWER(TRIM(%s)) LIMIT 1", (val,), fetch=True)
                                else:
                                    ex = execute_query("SELECT id, phone_numbers FROM properties WHERE LOWER(TRIM(apn)) = LOWER(TRIM(%s)) LIMIT 1", (val,), fetch=True)
                                if ex:
                                    lead = dict(ex[0])
                                    if overwrite:
                                        new_ph = ", ".join(phones)
                                    else:
                                        old_ph = [p.strip() for p in str(lead.get("phone_numbers") or "").split(",") if p.strip()]
                                        new_ph = ", ".join(dict.fromkeys(old_ph + phones))
                                    execute_query("UPDATE properties SET phone_numbers = %s WHERE id = %s", (new_ph, lead["id"]))
                                    matched += 1
                                else:
                                    not_found += 1
                            except Exception:
                                not_found += 1
                            prog_ph.progress((i+1)/len(ph_df))
                        st.success(f"✅ Updated **{matched:,}** leads · {not_found:,} not matched")
                except Exception as e:
                    st.error(f"Error: {e}")

        else:  # Manual
            search_term = st.text_input("🔍 Search lead by address or name", placeholder="123 Main St", key="ph_search_term")
            if search_term and len(search_term) >= 3:
                try:
                    results = execute_query(
                        "SELECT id, street_address, city, state, owner_name, phone_numbers FROM properties WHERE street_address ILIKE %s OR owner_name ILIKE %s LIMIT 20",
                        (f"%{search_term}%", f"%{search_term}%"), fetch=True
                    )
                    if results:
                        choices = {f"{r['street_address']}, {r['city']} — {r.get('owner_name') or 'Unknown'}": dict(r) for r in results}
                        picked = st.selectbox("Select lead", list(choices.keys()), key="ph_manual_pick")
                        if picked:
                            ld = choices[picked]
                            st.info(f"Current phones: **{ld.get('phone_numbers') or 'None'}**")
                            with st.form("manual_phone_form"):
                                mp1,mp2 = st.columns(2); mp3,mp4 = st.columns(2)
                                with mp1: new_ph1 = st.text_input("Phone 1", placeholder="555-123-4567")
                                with mp2: new_ph2 = st.text_input("Phone 2")
                                with mp3: new_ph3 = st.text_input("Phone 3")
                                with mp4: new_ph4 = st.text_input("Phone 4")
                                overwrite_m = st.checkbox("Replace existing (unchecked = append)", value=False)
                                if st.form_submit_button("💾 Save", type="primary"):
                                    new_phones = [p.strip() for p in [new_ph1,new_ph2,new_ph3,new_ph4] if p.strip()]
                                    if new_phones:
                                        if overwrite_m:
                                            final = ", ".join(new_phones)
                                        else:
                                            old = [p.strip() for p in str(ld.get("phone_numbers") or "").split(",") if p.strip()]
                                            final = ", ".join(dict.fromkeys(old + new_phones))
                                        execute_query("UPDATE properties SET phone_numbers = %s WHERE id = %s", (final, ld["id"]))
                                        st.success(f"✅ Saved: {final}")
                                        st.rerun()
                                    else:
                                        st.warning("Enter at least one phone number.")
                    else:
                        st.info("No leads found.")
                except Exception as e:
                    st.error(str(e))

    # ═══════════════════════════════════════════════════════════════
    # TAB 3 — MANUAL SINGLE LEAD ENTRY
    # ═══════════════════════════════════════════════════════════════
    with import_tab3:
        st.markdown('<div class="section-title">✏️ Add a Single Lead Manually</div>', unsafe_allow_html=True)
        with st.form("manual_lead_form", clear_on_submit=True):
            st.markdown("**📍 Property Location** *(required)*")
            ml1,ml2,ml3,ml4 = st.columns(4)
            with ml1: ml_addr  = st.text_input("Street Address *", placeholder="123 Main St")
            with ml2: ml_city  = st.text_input("City *", placeholder="Columbus")
            with ml3: ml_state = st.text_input("State *", placeholder="OH", max_chars=2)
            with ml4: ml_zip   = st.text_input("Zip", placeholder="43215")

            st.markdown("**👤 Owner**")
            mo1,mo2 = st.columns(2)
            with mo1: ml_first = st.text_input("First Name")
            with mo2: ml_last  = st.text_input("Last Name")

            st.markdown("**📞 Phone Numbers**")
            mp1c,mp2c,mp3c = st.columns(3)
            with mp1c: ml_ph1 = st.text_input("Phone 1", placeholder="555-123-4567")
            with mp2c: ml_ph2 = st.text_input("Phone 2")
            with mp3c: ml_ph3 = st.text_input("Phone 3")

            st.markdown("**🏠 Property Details** *(optional)*")
            pd1,pd2,pd3,pd4,pd5 = st.columns(5)
            with pd1: ml_type  = st.selectbox("Type", ["","Single-Family","Condo","Multi-Family","Land","Commercial"])
            with pd2: ml_beds  = st.number_input("Beds",  0, 20, 0)
            with pd3: ml_baths = st.number_input("Baths", 0, 20, 0)
            with pd4: ml_sqft  = st.number_input("SqFt",  0, 50000, 0, step=100)
            with pd5: ml_yr    = st.number_input("Year Built", 1800, 2025, 0)

            st.markdown("**💰 Financial** *(optional)*")
            pf1,pf2,pf3 = st.columns(3)
            with pf1: ml_value  = st.number_input("Est. Value ($)",  0, 10000000, 0, step=5000)
            with pf2: ml_equity = st.number_input("Est. Equity ($)", 0, 10000000, 0, step=5000)
            with pf3: ml_source = st.text_input("List Source", placeholder="e.g. Direct Mail March")

            ml_notes = st.text_area("Notes", placeholder="Any notes about this lead...")
            ml_stage = st.selectbox("Pipeline Stage", ["","New","Contacted","Negotiating","Closed","Lost"])

            if st.form_submit_button("➕ Add Lead", type="primary", use_container_width=True):
                if not ml_addr.strip() or not ml_city.strip() or not ml_state.strip():
                    st.error("Address, City, and State are required.")
                else:
                    phones = [p.strip() for p in [ml_ph1,ml_ph2,ml_ph3] if p.strip()]
                    payload = {"address": ml_addr.strip(), "city": ml_city.strip(), "state": ml_state.strip().upper()[:2]}
                    if ml_zip.strip():    payload["zip"]           = ml_zip.strip()
                    if ml_first.strip():  payload["owner_first"]   = ml_first.strip()
                    if ml_last.strip():   payload["owner_last"]    = ml_last.strip()
                    if phones:            payload["phone_numbers"] = ", ".join(phones)
                    if ml_type:           payload["property_type"] = ml_type
                    if ml_beds > 0:       payload["beds"]          = ml_beds
                    if ml_baths > 0:      payload["baths"]         = ml_baths
                    if ml_sqft > 0:       payload["living_sqft"]   = ml_sqft
                    if ml_yr > 1800:      payload["year_built"]    = ml_yr
                    if ml_value > 0:      payload["est_value"]     = ml_value
                    if ml_equity > 0:     payload["est_equity_amt"]= ml_equity
                    if ml_source.strip(): payload["source"]        = ml_source.strip()
                    if ml_notes.strip():  payload["notes"]         = ml_notes.strip()
                    if ml_stage:          payload["stage"]         = ml_stage
                    try:
                        stack_lead(payload)
                        st.success(f"✅ Lead added: {ml_addr.strip()}, {ml_city.strip()}, {ml_state.strip().upper()}")
                        st.balloons()
                    except Exception as e:
                        st.error(f"Error: {e}")

# ═══════════════════════════════════════════════════════════════
# PAGE: MY FILES
# ═══════════════════════════════════════════════════════════════
elif page_key == "My Files":
    st.markdown('<div class="page-title">My Files</div>', unsafe_allow_html=True)
    st.markdown('<div class="page-sub">Manage your uploaded lists and track their status.</div>', unsafe_allow_html=True)

    try:
        files = list_uploaded_lists()
        if not files:
            st.markdown("""
            <div class="re-card" style="text-align:center;padding:2.5rem;">
                <div style="font-size:2rem;margin-bottom:0.75rem;">📁</div>
                <div style="font-weight:600;color:#e6edf3;">No uploaded files yet</div>
                <div style="font-size:0.82rem;margin-top:0.4rem;color:#8b949e;">Import a CSV in the <strong>Import</strong> page to see it here.</div>
            </div>""", unsafe_allow_html=True)
        else:
            if st.button("🗑 Delete selected", type="secondary"):
                sel_ids = [f["id"] for f in files if st.session_state.get(f"myfiles_sel_{f['id']}", False)]
                if sel_ids:
                    delete_uploaded_lists(sel_ids)
                    st.success(f"Deleted {len(sel_ids)} file(s).")
                    st.rerun()
                else:
                    st.warning("Select at least one file to delete.")

            st.divider()
            # Header row
            h0,h1,h2,h3,h4,h5 = st.columns([0.4,2.5,1.5,0.8,1.5,0.6])
            for h, label in zip([h0,h1,h2,h3,h4,h5], ["","List Name","Uploaded","Leads","Status","Action"]):
                h.markdown(f"<div style='font-size:0.7rem;font-weight:700;letter-spacing:0.07em;text-transform:uppercase;color:#8b949e;'>{label}</div>", unsafe_allow_html=True)
            st.divider()

            for f in files:
                c0,c1,c2,c3,c4,c5 = st.columns([0.4,2.5,1.5,0.8,1.5,0.6])
                with c0: st.checkbox("", value=False, key=f"myfiles_sel_{f['id']}", label_visibility="collapsed")
                with c1:
                    st.markdown(f"**{f['name']}**")
                    st.caption(f['filename'])
                with c2: st.caption(str(f['uploaded_at'])[:16])
                with c3: st.caption(f"{f['lead_count']:,}")
                with c4:
                    new_status = st.selectbox("", UPLOAD_STATUSES,
                                              index=UPLOAD_STATUSES.index(f["status"]) if f["status"] in UPLOAD_STATUSES else 0,
                                              key=f"file_status_{f['id']}", label_visibility="collapsed")
                with c5:
                    if st.button("Save", key=f"file_update_{f['id']}", use_container_width=True):
                        update_uploaded_list_status(f["id"], new_status)
                        st.success("Updated.")
                        st.rerun()
                st.divider()
    except Exception as e:
        st.error(str(e)); st.exception(e)


# ═══════════════════════════════════════════════════════════════
# PAGE: TAGS
# ═══════════════════════════════════════════════════════════════
elif page_key == "Tags":
    st.markdown('<div class="page-title">Tag Manager</div>', unsafe_allow_html=True)
    st.markdown('<div class="page-sub">View, rename, and remove tags across your lead database.</div>', unsafe_allow_html=True)

    if "tags" not in _cols:
        st.warning("No `tags` column found. Run: `ALTER TABLE properties ADD COLUMN tags TEXT;`")
    else:
        try:
            tag_list = get_all_tags_with_counts()
            if not tag_list:
                st.info("No tags yet. Add tags from Lead Engine → batch Tag action.")
            else:
                # Summary
                t_total = sum(t["cnt"] for t in tag_list)
                tm1,tm2 = st.columns(2)
                tm1.markdown(f'<div class="metric-tile"><div class="label">Unique Tags</div><div class="value">{len(tag_list)}</div></div>', unsafe_allow_html=True)
                tm2.markdown(f'<div class="metric-tile"><div class="label">Total Tag Assignments</div><div class="value">{t_total:,}</div></div>', unsafe_allow_html=True)

                st.markdown("<div style='margin-top:1rem;'></div>", unsafe_allow_html=True)
                st.markdown('<div class="section-title">All Tags</div>', unsafe_allow_html=True)
                tag_df = pd.DataFrame(tag_list)
                st.dataframe(tag_df.rename(columns={"tag_name":"Tag","cnt":"Leads"}),
                             use_container_width=True, hide_index=True)

                st.divider()
                chosen = st.selectbox("Select a tag to manage", [t["tag_name"] for t in tag_list])
                chosen_cnt = next((t["cnt"] for t in tag_list if t["tag_name"] == chosen), 0)

                # View leads
                st.markdown(f'<div class="section-title">Leads with tag «{chosen}» ({chosen_cnt:,})</div>', unsafe_allow_html=True)
                if st.button("View leads", type="primary"):
                    st.session_state["tag_show"] = chosen
                    st.rerun()
                if st.session_state.get("tag_show") == chosen:
                    leads = get_leads_by_tag(chosen)
                    if leads:
                        st.dataframe(pd.DataFrame(leads), use_container_width=True, hide_index=True)
                    else:
                        st.info("No leads found.")

                st.divider()
                st.markdown('<div class="section-title">Rename or Remove</div>', unsafe_allow_html=True)
                rc1, rc2 = st.columns(2)
                with rc1:
                    new_name = st.text_input("Rename to", placeholder="New tag name", key="tag_new_name")
                    if st.button("Rename", type="primary") and new_name.strip():
                        n = rename_tag(chosen, new_name.strip())
                        st.success(f"Renamed in {n} lead(s).")
                        st.rerun()
                with rc2:
                    st.markdown("<div style='margin-top:1.9rem;'></div>", unsafe_allow_html=True)
                    if st.button("🗑 Remove from all leads", type="secondary"):
                        n = remove_tag_from_all(chosen)
                        st.success(f"Removed from {n} lead(s).")
                        st.rerun()
        except Exception as e:
            st.error(str(e)); st.exception(e)
