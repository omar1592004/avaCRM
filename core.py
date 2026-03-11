import os
import psycopg2
from psycopg2.extras import RealDictCursor

# ----------------------------------------------------------------
# DB CONFIG — supports Neon connection string OR individual keys
# ----------------------------------------------------------------
_defaults = {
    "host": "localhost",
    "port": "5432",
    "dbname": "re_engine",
    "user": "postgres",
    "password": "",
    "sslmode": "prefer",
}

try:
    import streamlit as st
    if getattr(st, "secrets", None):
        # Option 1: full connection string (Neon style)
        if "database_url" in st.secrets:
            os.environ.setdefault("DATABASE_URL", st.secrets["database_url"])
        # Option 2: [postgres] section
        elif "postgres" in st.secrets:
            pg = st.secrets["postgres"]
            _defaults = {
                "host":     pg.get("host",     _defaults["host"]),
                "port":     str(pg.get("port", _defaults["port"])),
                "dbname":   pg.get("dbname",   _defaults["dbname"]),
                "user":     pg.get("user",      _defaults["user"]),
                "password": pg.get("password", _defaults["password"]),
                "sslmode":  pg.get("sslmode",  "require"),
            }
        # Option 3: flat keys
        else:
            _defaults = {
                "host":     st.secrets.get("db_host",     _defaults["host"]),
                "port":     str(st.secrets.get("db_port", _defaults["port"])),
                "dbname":   st.secrets.get("db_name",     _defaults["dbname"]),
                "user":     st.secrets.get("db_user",     _defaults["user"]),
                "password": st.secrets.get("db_password", _defaults["password"]),
                "sslmode":  st.secrets.get("sslmode",     "require"),
            }
except Exception:
    pass

# Environment variable DATABASE_URL takes highest priority (Neon connection string)
DATABASE_URL = os.environ.get("DATABASE_URL", "")

DB_CONFIG = {
    "host":     os.environ.get("DB_HOST",     _defaults["host"]),
    "port":     os.environ.get("DB_PORT",     _defaults["port"]),
    "dbname":   os.environ.get("DB_NAME",     _defaults["dbname"]),
    "user":     os.environ.get("DB_USER",     _defaults["user"]),
    "password": os.environ.get("DB_PASSWORD", _defaults["password"]),
    "sslmode":  os.environ.get("DB_SSLMODE",  _defaults.get("sslmode", "require")),
}


def get_db_connection():
    """Return a psycopg2 connection. Prefers DATABASE_URL if set."""
    if DATABASE_URL:
        return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
    return psycopg2.connect(
        host=DB_CONFIG["host"],
        port=DB_CONFIG["port"],
        dbname=DB_CONFIG["dbname"],
        user=DB_CONFIG["user"],
        password=DB_CONFIG["password"],
        sslmode=DB_CONFIG["sslmode"],
    )


def execute_query(query, params=None, fetch=False):
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(query, params)
                if fetch:
                    return cur.fetchall()
                conn.commit()
                return True
    except Exception as e:
        print(f"Database error: {str(e)}")
        print(f"Query: {query}")
        print(f"Params: {params}")
        raise e


def get_table_schema():
    query = """
    SELECT column_name, data_type, is_nullable
    FROM information_schema.columns
    WHERE table_name = 'properties'
    ORDER BY ordinal_position;
    """
    return execute_query(query, fetch=True)


def _properties_columns():
    schema = get_table_schema()
    return {r["column_name"] for r in schema} if schema else set()


def stack_lead(payload):
    try:
        cols = _properties_columns()
        owner_name = None
        if payload.get('owner_first') and payload.get('owner_last'):
            owner_name = f"{payload['owner_first']} {payload['owner_last']}".strip()
        elif payload.get('owner_first'):
            owner_name = payload['owner_first']
        elif payload.get('owner_last'):
            owner_name = payload['owner_last']
        elif payload.get('owner'):
            owner_name = payload['owner']

        check_query = """
        SELECT id, motivation_score
        FROM properties
        WHERE LOWER(TRIM(street_address)) = LOWER(TRIM(%s))
        AND LOWER(TRIM(city)) = LOWER(TRIM(%s))
        AND LOWER(TRIM(state)) = LOWER(TRIM(%s))
        """
        existing = execute_query(
            check_query,
            (payload['address'], payload['city'], payload['state']),
            fetch=True
        )

        if existing and len(existing) > 0:
            update_parts = []
            update_params = []
            update_parts.append("motivation_score = COALESCE(motivation_score, 0) + 1")
            update_parts.append("last_list_source = %s")
            update_params.append(payload.get('source', 'Import'))

            if payload.get('zip'):
                update_parts.append("zip_code = %s")
                update_params.append(payload['zip'])
            if owner_name:
                update_parts.append("owner_name = %s")
                update_params.append(owner_name)
            if payload.get('apn'):
                update_parts.append("apn = %s")
                update_params.append(payload['apn'])
            if payload.get('phone_numbers'):
                update_parts.append("phone_numbers = %s")
                update_params.append(payload['phone_numbers'])
            if payload.get('property_type') is not None:
                update_parts.append("property_type = %s")
                update_params.append(payload['property_type'])
            if payload.get('beds') is not None:
                update_parts.append("beds = %s")
                update_params.append(payload['beds'])
            if payload.get('baths') is not None:
                update_parts.append("baths = %s")
                update_params.append(payload['baths'])
            if payload.get('occupancy_status') is not None:
                update_parts.append("occupancy_status = %s")
                update_params.append(payload['occupancy_status'])
            if payload.get('est_value') is not None:
                update_parts.append("est_value = %s")
                update_params.append(payload['est_value'])
            if payload.get('last_sale_price') is not None:
                update_parts.append("last_sale_price = %s")
                update_params.append(payload['last_sale_price'])
            if payload.get('county') is not None and 'county' in cols:
                update_parts.append("county = %s")
                update_params.append(payload['county'])
            if payload.get('mailing_address') is not None and 'mailing_address' in cols:
                update_parts.append("mailing_address = %s")
                update_params.append(payload['mailing_address'])
            if payload.get('mailing_city') is not None and 'mailing_city' in cols:
                update_parts.append("mailing_city = %s")
                update_params.append(payload['mailing_city'])
            if payload.get('mailing_state') is not None and 'mailing_state' in cols:
                update_parts.append("mailing_state = %s")
                update_params.append(payload['mailing_state'])
            if payload.get('mailing_zip') is not None and 'mailing_zip' in cols:
                update_parts.append("mailing_zip = %s")
                update_params.append(payload['mailing_zip'])

            optional_fields = [
                ('property_use', 'property_use'), ('land_use', 'land_use'), ('subdivision', 'subdivision'),
                ('legal_description', 'legal_description'), ('living_sqft', 'living_sqft'), ('lot_acres', 'lot_acres'),
                ('lot_sqft', 'lot_sqft'), ('year_built', 'year_built'), ('stories', 'stories'),
                ('units_count', 'units_count'), ('fireplaces', 'fireplaces'), ('garage_type', 'garage_type'),
                ('garage_sqft', 'garage_sqft'), ('carport', 'carport'), ('carport_area', 'carport_area'),
                ('ac_type', 'ac_type'), ('heating_type', 'heating_type'),
                ('ownership_length_months', 'ownership_length_months'),
                ('owner_type', 'owner_type'), ('owner_occupied', 'owner_occupied'), ('vacant', 'vacant')
            ]
            for payload_key, col_name in optional_fields:
                if payload.get(payload_key) is not None and col_name in cols:
                    update_parts.append(f"{col_name} = %s")
                    update_params.append(payload[payload_key])

            update_query = f"""
            UPDATE properties
            SET {', '.join(update_parts)}
            WHERE id = %s
            """
            update_params.append(existing[0]['id'])
            execute_query(update_query, update_params)
            return {"action": "updated", "id": existing[0]['id']}

        else:
            insert_columns, insert_placeholders, insert_params = [], [], []

            insert_columns.append("street_address"); insert_placeholders.append("%s"); insert_params.append(payload['address'])
            insert_columns.append("city");           insert_placeholders.append("%s"); insert_params.append(payload['city'])
            insert_columns.append("state");          insert_placeholders.append("%s"); insert_params.append(payload['state'])
            insert_columns.append("motivation_score"); insert_placeholders.append("1")
            insert_columns.append("last_list_source"); insert_placeholders.append("%s"); insert_params.append(payload.get('source', 'Import'))

            if payload.get('zip'):
                insert_columns.append("zip_code"); insert_placeholders.append("%s"); insert_params.append(payload['zip'])
            if owner_name:
                insert_columns.append("owner_name"); insert_placeholders.append("%s"); insert_params.append(owner_name)
            if payload.get('apn'):
                insert_columns.append("apn"); insert_placeholders.append("%s"); insert_params.append(payload['apn'])
            if payload.get('phone_numbers'):
                insert_columns.append("phone_numbers"); insert_placeholders.append("%s"); insert_params.append(payload['phone_numbers'])
            if payload.get('property_type') is not None:
                insert_columns.append("property_type"); insert_placeholders.append("%s"); insert_params.append(payload['property_type'])
            if payload.get('beds') is not None:
                insert_columns.append("beds"); insert_placeholders.append("%s"); insert_params.append(payload['beds'])
            if payload.get('baths') is not None:
                insert_columns.append("baths"); insert_placeholders.append("%s"); insert_params.append(payload['baths'])
            if payload.get('occupancy_status') is not None:
                insert_columns.append("occupancy_status"); insert_placeholders.append("%s"); insert_params.append(payload['occupancy_status'])
            if payload.get('est_value') is not None:
                insert_columns.append("est_value"); insert_placeholders.append("%s"); insert_params.append(payload['est_value'])
            if payload.get('last_sale_price') is not None:
                insert_columns.append("last_sale_price"); insert_placeholders.append("%s"); insert_params.append(payload['last_sale_price'])
            if payload.get('county') is not None and 'county' in cols:
                insert_columns.append("county"); insert_placeholders.append("%s"); insert_params.append(payload['county'])
            if payload.get('mailing_address') is not None and 'mailing_address' in cols:
                insert_columns.append("mailing_address"); insert_placeholders.append("%s"); insert_params.append(payload['mailing_address'])
            if payload.get('mailing_city') is not None and 'mailing_city' in cols:
                insert_columns.append("mailing_city"); insert_placeholders.append("%s"); insert_params.append(payload['mailing_city'])
            if payload.get('mailing_state') is not None and 'mailing_state' in cols:
                insert_columns.append("mailing_state"); insert_placeholders.append("%s"); insert_params.append(payload['mailing_state'])
            if payload.get('mailing_zip') is not None and 'mailing_zip' in cols:
                insert_columns.append("mailing_zip"); insert_placeholders.append("%s"); insert_params.append(payload['mailing_zip'])

            optional_fields = [
                ('property_use', 'property_use'), ('land_use', 'land_use'), ('subdivision', 'subdivision'),
                ('legal_description', 'legal_description'), ('living_sqft', 'living_sqft'), ('lot_acres', 'lot_acres'),
                ('lot_sqft', 'lot_sqft'), ('year_built', 'year_built'), ('stories', 'stories'),
                ('units_count', 'units_count'), ('fireplaces', 'fireplaces'), ('garage_type', 'garage_type'),
                ('garage_sqft', 'garage_sqft'), ('carport', 'carport'), ('carport_area', 'carport_area'),
                ('ac_type', 'ac_type'), ('heating_type', 'heating_type'),
                ('ownership_length_months', 'ownership_length_months'),
                ('owner_type', 'owner_type'), ('owner_occupied', 'owner_occupied'), ('vacant', 'vacant')
            ]
            for payload_key, col_name in optional_fields:
                if payload.get(payload_key) is not None and col_name in cols:
                    insert_columns.append(col_name); insert_placeholders.append("%s"); insert_params.append(payload[payload_key])

            insert_query = f"""
            INSERT INTO properties ({', '.join(insert_columns)})
            VALUES ({', '.join(insert_placeholders)})
            """
            execute_query(insert_query, insert_params)
            return {"action": "inserted"}

    except Exception as e:
        print(f"Error in stack_lead: {str(e)}")
        raise e


def bulk_insert_leads(payloads):
    if not payloads:
        return 0
    cols = _properties_columns()
    rows = []
    for payload in payloads:
        owner_name = None
        if payload.get("owner_first") and payload.get("owner_last"):
            owner_name = f"{payload['owner_first']} {payload['owner_last']}".strip()
        elif payload.get("owner_first"):
            owner_name = payload["owner_first"]
        elif payload.get("owner_last"):
            owner_name = payload["owner_last"]
        elif payload.get("owner_name"):
            owner_name = payload["owner_name"]

        row = {
            "street_address": payload.get("address", ""),
            "city":           payload.get("city", ""),
            "state":          payload.get("state", ""),
            "motivation_score": 1,
            "last_list_source": payload.get("source", "Import"),
        }
        if payload.get("zip"):             row["zip_code"]       = payload["zip"]
        if owner_name:                     row["owner_name"]     = owner_name
        if payload.get("apn"):             row["apn"]            = payload["apn"]
        if payload.get("phone_numbers"):   row["phone_numbers"]  = payload["phone_numbers"]
        if payload.get("property_type"):   row["property_type"]  = payload["property_type"]
        if payload.get("beds") is not None:   row["beds"]        = payload["beds"]
        if payload.get("baths") is not None:  row["baths"]       = payload["baths"]
        if payload.get("occupancy_status"):   row["occupancy_status"] = payload["occupancy_status"]
        if payload.get("est_value") is not None:       row["est_value"]      = payload["est_value"]
        if payload.get("last_sale_price") is not None: row["last_sale_price"] = payload["last_sale_price"]
        if payload.get("county") and "county" in cols:                   row["county"]           = payload["county"]
        if payload.get("mailing_address") and "mailing_address" in cols: row["mailing_address"]  = payload["mailing_address"]
        if payload.get("mailing_city")    and "mailing_city"    in cols: row["mailing_city"]     = payload["mailing_city"]
        if payload.get("mailing_state")   and "mailing_state"   in cols: row["mailing_state"]    = payload["mailing_state"]
        if payload.get("mailing_zip")     and "mailing_zip"     in cols: row["mailing_zip"]      = payload["mailing_zip"]

        optional_fields = [
            "property_use", "land_use", "subdivision", "legal_description",
            "living_sqft", "lot_acres", "lot_sqft", "year_built", "stories",
            "units_count", "fireplaces", "garage_type", "garage_sqft",
            "carport", "carport_area", "ac_type", "heating_type",
            "ownership_length_months", "owner_type", "owner_occupied", "vacant",
        ]
        for f in optional_fields:
            if payload.get(f) is not None and f in cols:
                row[f] = payload[f]

        rows.append(row)

    all_cols = []
    for row in rows:
        for k in row:
            if k not in all_cols:
                all_cols.append(k)

    placeholders, all_params = [], []
    for row in rows:
        row_vals = [row.get(c) for c in all_cols]
        placeholders.append("(" + ", ".join(["%s"] * len(all_cols)) + ")")
        all_params.extend(row_vals)

    query = f"""
        INSERT INTO properties ({', '.join(all_cols)})
        VALUES {', '.join(placeholders)}
    """
    execute_query(query, all_params)
    return len(rows)


def get_pipeline_counts():
    q = """
    SELECT COALESCE(NULLIF(TRIM(stage), ''), 'Unset') AS stage, COUNT(*) AS cnt
    FROM properties
    GROUP BY COALESCE(NULLIF(TRIM(stage), ''), 'Unset')
    ORDER BY cnt DESC
    """
    return execute_query(q, fetch=True)


def get_leads_by_stage(stage_filter=None):
    if stage_filter is None or stage_filter == "All":
        return execute_query("SELECT * FROM properties ORDER BY id", fetch=True)
    if stage_filter == "Unset":
        return execute_query(
            "SELECT * FROM properties WHERE (stage IS NULL OR TRIM(stage) = '') ORDER BY id",
            fetch=True
        )
    return execute_query(
        "SELECT * FROM properties WHERE TRIM(stage) = %s ORDER BY id",
        (stage_filter,), fetch=True
    )


def update_stage(lead_ids, new_stage):
    if not lead_ids:
        return 0
    placeholders = ",".join(["%s"] * len(lead_ids))
    q = f"UPDATE properties SET stage = %s WHERE id IN ({placeholders})"
    execute_query(q, [new_stage] + list(lead_ids))
    return len(lead_ids)


def get_all_tags_with_counts():
    rows = execute_query(
        "SELECT id, tags FROM properties WHERE tags IS NOT NULL AND TRIM(tags) != ''",
        fetch=True
    )
    from collections import Counter
    counter = Counter()
    for r in rows:
        for t in (r["tags"] or "").split(","):
            t = t.strip()
            if t:
                counter[t] += 1
    return [{"tag_name": k, "cnt": v} for k, v in counter.most_common()]


def rename_tag(old_name, new_name):
    rows = execute_query(
        "SELECT id, tags FROM properties WHERE tags IS NOT NULL AND TRIM(tags) != ''",
        fetch=True
    )
    updated = 0
    for r in rows:
        tags = [t.strip() for t in (r["tags"] or "").split(",") if t.strip()]
        new_tags = [new_name if t.lower() == old_name.lower() else t for t in tags]
        if new_tags != tags:
            execute_query("UPDATE properties SET tags = %s WHERE id = %s", (", ".join(new_tags), r["id"]))
            updated += 1
    return updated


def get_leads_by_tag(tag_name):
    if not tag_name or not str(tag_name).strip():
        return []
    q = """
        SELECT * FROM properties
        WHERE LOWER(TRIM(%s)) IN (
            SELECT LOWER(TRIM(t))
            FROM unnest(string_to_array(COALESCE(tags, ''), ',')) AS t
            WHERE TRIM(t) != ''
        )
        ORDER BY street_address
    """
    return execute_query(q, (str(tag_name).strip(),), fetch=True) or []


def remove_tag_from_all(tag_name):
    rows = execute_query(
        "SELECT id, tags FROM properties WHERE tags IS NOT NULL AND TRIM(tags) != ''",
        fetch=True
    )
    updated = 0
    for r in rows:
        tags = [t.strip() for t in (r["tags"] or "").split(",") if t.strip()]
        new_tags = [t for t in tags if t.lower() != tag_name.lower()]
        if len(new_tags) < len(tags):
            execute_query(
                "UPDATE properties SET tags = %s WHERE id = %s",
                (", ".join(new_tags) if new_tags else None, r["id"])
            )
            updated += 1
    return updated


# ---------- Saved Searches ----------
def _ensure_saved_searches_table():
    execute_query("""
        CREATE TABLE IF NOT EXISTS saved_searches (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            filters_json TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)


def save_saved_search(name, filters_json):
    _ensure_saved_searches_table()
    execute_query(
        "INSERT INTO saved_searches (name, filters_json) VALUES (%s, %s)",
        (name.strip(), filters_json)
    )


def list_saved_searches():
    _ensure_saved_searches_table()
    rows = execute_query(
        "SELECT id, name, created_at FROM saved_searches ORDER BY created_at DESC",
        fetch=True
    )
    return rows or []


def get_saved_search(search_id):
    rows = execute_query(
        "SELECT name, filters_json FROM saved_searches WHERE id = %s",
        (search_id,), fetch=True
    )
    return rows[0] if rows else None


def delete_saved_search(search_id):
    execute_query("DELETE FROM saved_searches WHERE id = %s", (search_id,))


# ---------- Lead Activities ----------
def _ensure_lead_activities_table():
    execute_query("""
        CREATE TABLE IF NOT EXISTS lead_activities (
            id SERIAL PRIMARY KEY,
            property_id INTEGER NOT NULL,
            activity_type VARCHAR(50) NOT NULL DEFAULT 'note',
            content TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    try:
        execute_query("""
            CREATE INDEX IF NOT EXISTS idx_lead_activities_property_id
            ON lead_activities (property_id)
        """)
    except Exception:
        pass


def add_lead_activity(property_id, activity_type="note", content=None):
    _ensure_lead_activities_table()
    execute_query(
        "INSERT INTO lead_activities (property_id, activity_type, content) VALUES (%s, %s, %s)",
        (int(property_id), (activity_type or "note").strip()[:50], (content or "").strip() or None)
    )


def get_lead_activities(property_id):
    _ensure_lead_activities_table()
    rows = execute_query(
        "SELECT id, property_id, activity_type, content, created_at FROM lead_activities WHERE property_id = %s ORDER BY created_at DESC",
        (int(property_id),), fetch=True
    )
    return rows or []


# ---------- Dashboard ----------
def get_dashboard_stats():
    total = execute_query("SELECT COUNT(*) AS c FROM properties", fetch=True)
    total = total[0]["c"] if total else 0
    by_state = []
    for state_col in ("state", "property_state"):
        try:
            by_state = execute_query(
                f"SELECT {state_col} AS state, COUNT(*) AS cnt FROM properties WHERE {state_col} IS NOT NULL AND TRIM({state_col}) != '' GROUP BY {state_col} ORDER BY cnt DESC",
                fetch=True
            )
            by_state = by_state or []
            break
        except Exception:
            continue
    try:
        by_stage = get_pipeline_counts()
    except Exception:
        by_stage = []
    return {"total": total, "by_state": by_state, "by_stage": by_stage}


# ---------- Uploaded Lists ----------
UPLOAD_STATUSES = ("new", "closed", "negotiating", "contacted", "lost", "interesting")


def _ensure_uploaded_lists_table():
    execute_query("""
        CREATE TABLE IF NOT EXISTS uploaded_lists (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            filename VARCHAR(255) NOT NULL,
            uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            status VARCHAR(50) DEFAULT 'new'
        )
    """)


def add_uploaded_list(name, filename):
    _ensure_uploaded_lists_table()
    execute_query(
        "INSERT INTO uploaded_lists (name, filename, status) VALUES (%s, %s, 'new')",
        (name.strip() or filename, filename)
    )


def list_uploaded_lists():
    _ensure_uploaded_lists_table()
    rows = execute_query(
        "SELECT id, name, filename, uploaded_at, status FROM uploaded_lists ORDER BY uploaded_at DESC",
        fetch=True
    )
    if not rows:
        return []
    for r in rows:
        r["lead_count"] = 0
        try:
            cnt = execute_query(
                "SELECT COUNT(*) AS c FROM properties WHERE last_list_source = %s",
                (r["name"],), fetch=True
            )
            if cnt:
                r["lead_count"] = cnt[0]["c"]
        except Exception:
            pass
    return rows


def update_uploaded_list_status(list_id, status):
    if status not in UPLOAD_STATUSES:
        raise ValueError(f"Status must be one of {UPLOAD_STATUSES}")
    execute_query("UPDATE uploaded_lists SET status = %s WHERE id = %s", (status, list_id))


def delete_uploaded_lists(ids):
    if not ids:
        return
    placeholders = ",".join(["%s"] * len(ids))
    execute_query(f"DELETE FROM uploaded_lists WHERE id IN ({placeholders})", list(ids))


# ---------- Clear data ----------
def count_properties(state_filter=None):
    if state_filter is None or state_filter in ("All", ""):
        try:
            r = execute_query("SELECT COUNT(*) AS c FROM properties", fetch=True)
            return r[0]["c"] if r else 0
        except Exception:
            return 0
    for col in ("state", "property_state"):
        try:
            r = execute_query(f"SELECT COUNT(*) AS c FROM properties WHERE {col} = %s", (state_filter,), fetch=True)
            if r:
                return r[0]["c"]
        except Exception:
            continue
    return 0


def delete_properties(state_filter=None):
    if state_filter is None or state_filter in ("All", ""):
        execute_query("DELETE FROM properties")
        return
    for col in ("state", "property_state"):
        try:
            execute_query(f"DELETE FROM properties WHERE {col} = %s", (state_filter,))
            return
        except Exception:
            continue


# ================================================================
# UPGRADE 1: LIST STACKING — find leads on multiple imported lists
# ================================================================

def get_stacked_leads(min_lists=2):
    """
    Returns leads that appear on more than min_lists imported lists.
    These are 'High Priority' stacked leads.
    """
    try:
        rows = execute_query("""
            SELECT
                p.id,
                p.street_address,
                p.city,
                p.state,
                p.owner_name,
                p.phone_numbers,
                p.est_value,
                p.est_equity_pct,
                p.motivation_score,
                p.stage,
                p.tags,
                COUNT(DISTINCT p.last_list_source) AS list_count,
                STRING_AGG(DISTINCT p.last_list_source, ' | ') AS list_names
            FROM properties p
            WHERE p.last_list_source IS NOT NULL AND TRIM(p.last_list_source) != ''
            GROUP BY
                p.id, p.street_address, p.city, p.state, p.owner_name,
                p.phone_numbers, p.est_value, p.est_equity_pct,
                p.motivation_score, p.stage, p.tags
            HAVING COUNT(DISTINCT p.last_list_source) >= %s
            ORDER BY list_count DESC, p.motivation_score DESC NULLS LAST
        """, (min_lists,), fetch=True)
        return rows or []
    except Exception as e:
        print(f"get_stacked_leads error: {e}")
        return []


def get_list_stack_summary():
    """Returns count of stacked leads by overlap count."""
    try:
        rows = execute_query("""
            SELECT list_count, COUNT(*) AS lead_count
            FROM (
                SELECT COUNT(DISTINCT last_list_source) AS list_count
                FROM properties
                WHERE last_list_source IS NOT NULL AND TRIM(last_list_source) != ''
                GROUP BY LOWER(TRIM(street_address)), LOWER(TRIM(city))
                HAVING COUNT(DISTINCT last_list_source) >= 2
            ) sub
            GROUP BY list_count
            ORDER BY list_count DESC
        """, fetch=True)
        return rows or []
    except Exception as e:
        print(f"get_list_stack_summary error: {e}")
        return []


# ================================================================
# UPGRADE 2: ADVANCED DISTRESS / MOTIVATION SCORING
# ================================================================

def calculate_distress_score(lead: dict) -> int:
    """
    Calculate a Distress Score (1-10) from lead attributes.
    Higher = more motivated seller.

    Triggers:
      +2  Absentee owner (mailing != property address)
      +2  High equity (est_equity_pct >= 40%)
      +2  Tax delinquent (tax_delinquent_year is set)
      +1  Vacant property
      +1  Long ownership (ownership_length_months >= 120 = 10 years)
      +1  Private/hard money loan
      +1  Pre-foreclosure / NOD flag
    """
    score = 0

    # Absentee: mailing state differs from property state
    prop_state = (lead.get("state") or lead.get("property_state") or "").strip().upper()
    mail_state = (lead.get("mailing_state") or "").strip().upper()
    if mail_state and prop_state and mail_state != prop_state:
        score += 2
    elif lead.get("is_absentee"):
        score += 2

    # High equity
    try:
        equity_pct = float(lead.get("est_equity_pct") or 0)
        if equity_pct >= 60:
            score += 2
        elif equity_pct >= 40:
            score += 1
    except (ValueError, TypeError):
        pass

    # Tax delinquent
    if lead.get("tax_delinquent_year"):
        score += 2

    # Vacant
    vacant = lead.get("vacant")
    occ = str(lead.get("occupancy_status") or lead.get("occupancy") or "").lower()
    if vacant is True or "vacant" in occ:
        score += 1

    # Long ownership
    try:
        months = int(lead.get("ownership_length_months") or 0)
        if months >= 120:
            score += 1
    except (ValueError, TypeError):
        pass

    # Private loan / hard money
    if lead.get("has_private_loan"):
        score += 1

    # Pre-foreclosure flag
    preforeclosure_keywords = ["foreclosure", "nod", "lis pendens", "pre-foreclosure"]
    tags_str = str(lead.get("tags") or "").lower()
    prop_type = str(lead.get("property_type") or "").lower()
    if any(k in tags_str or k in prop_type for k in preforeclosure_keywords):
        score += 1

    return min(max(score, 1), 10)


def batch_update_distress_scores(state_filter=None):
    """
    Recalculates and saves distress scores for all (or filtered) leads.
    Returns count of updated records.
    """
    try:
        query = "SELECT * FROM properties"
        params = []
        if state_filter:
            query += " WHERE state = %s OR property_state = %s"
            params = [state_filter, state_filter]

        rows = execute_query(query, params or None, fetch=True)
        if not rows:
            return 0

        updated = 0
        for lead in rows:
            lead_dict = dict(lead)
            score = calculate_distress_score(lead_dict)
            execute_query(
                "UPDATE properties SET motivation_score = %s WHERE id = %s",
                (score, lead_dict["id"])
            )
            updated += 1
        return updated
    except Exception as e:
        print(f"batch_update_distress_scores error: {e}")
        return 0


def get_score_distribution():
    """Returns motivation score distribution for charts."""
    try:
        rows = execute_query("""
            SELECT
                motivation_score AS score,
                COUNT(*) AS count
            FROM properties
            WHERE motivation_score IS NOT NULL
            GROUP BY motivation_score
            ORDER BY motivation_score DESC
        """, fetch=True)
        return rows or []
    except Exception as e:
        return []


# ================================================================
# UPGRADE 3: SKIP TRACING HOOK
# ================================================================

def skip_trace_lead(lead_id: int, provider: str = "batch_skip_tracing") -> dict:
    """
    Skip trace a single lead to get phone/email data.

    Supported providers (set via Streamlit secrets):
      - "batch_skip_tracing"  → batchskiptracing.com API
      - "skip_genie"          → skipgenie.com API
      - "prop_stream"         → propstream.com API

    Returns dict with keys: success, phones, emails, error
    Usage: Configure API key in .streamlit/secrets.toml:
        [skip_trace]
        provider = "batch_skip_tracing"
        api_key  = "YOUR_KEY_HERE"
    """
    result = {"success": False, "phones": [], "emails": [], "lead_id": lead_id, "error": None}

    try:
        import streamlit as st
        config = st.secrets.get("skip_trace", {})
        api_key = config.get("api_key", "")
        if not api_key:
            result["error"] = "No skip trace API key configured in secrets.toml [skip_trace] api_key"
            return result

        # Fetch lead data
        lead = execute_query("SELECT * FROM properties WHERE id = %s", (lead_id,), fetch=True)
        if not lead:
            result["error"] = f"Lead {lead_id} not found"
            return result
        lead = dict(lead[0])

        # ── BatchSkipTracing ─────────────────────────────────────
        if provider == "batch_skip_tracing":
            import requests
            payload = {
                "firstName":  lead.get("owner_first", ""),
                "lastName":   lead.get("owner_last", ""),
                "address":    lead.get("street_address", ""),
                "city":       lead.get("city", ""),
                "state":      lead.get("state") or lead.get("property_state", ""),
                "zip":        lead.get("zip_code", ""),
                "mailingAddress": lead.get("mailing_address", ""),
                "mailingCity":    lead.get("mailing_city", ""),
                "mailingState":   lead.get("mailing_state", ""),
                "mailingZip":     lead.get("mailing_zip", ""),
            }
            resp = requests.post(
                "https://api.batchskiptracing.com/api/search",
                json={"records": [payload]},
                headers={"Content-Type": "application/json", "api-key": api_key},
                timeout=15
            )
            resp.raise_for_status()
            data = resp.json()
            phones = []
            emails = []
            if data.get("output"):
                rec = data["output"][0] if data["output"] else {}
                for key in ["phone1","phone2","phone3","phone4","phone5","phone6","phone7","phone8","phone9","phone10"]:
                    ph = rec.get(key)
                    if ph and str(ph).strip() not in ("", "None", "null"):
                        phones.append(str(ph).strip())
                for key in ["email1","email2","email3"]:
                    em = rec.get(key)
                    if em and str(em).strip() not in ("", "None", "null"):
                        emails.append(str(em).strip())

            if phones:
                # Save back to DB
                existing = lead.get("phone_numbers") or ""
                all_phones = list(dict.fromkeys([p.strip() for p in existing.split(",") if p.strip()] + phones))
                execute_query(
                    "UPDATE properties SET phone_numbers = %s WHERE id = %s",
                    (", ".join(all_phones), lead_id)
                )
            result.update({"success": True, "phones": phones, "emails": emails})

        # ── SkipGenie ────────────────────────────────────────────
        elif provider == "skip_genie":
            import requests
            resp = requests.post(
                "https://api.skipgenie.com/v1/search",
                json={
                    "first_name": lead.get("owner_first", ""),
                    "last_name":  lead.get("owner_last", ""),
                    "address":    lead.get("street_address", ""),
                    "city":       lead.get("city", ""),
                    "state":      lead.get("state") or lead.get("property_state", ""),
                    "zip":        lead.get("zip_code", ""),
                },
                headers={"Authorization": f"Bearer {api_key}"},
                timeout=15
            )
            resp.raise_for_status()
            data = resp.json()
            phones = [p.get("number") for p in data.get("phones", []) if p.get("number")]
            emails = [e.get("address") for e in data.get("emails", []) if e.get("address")]
            result.update({"success": True, "phones": phones, "emails": emails})

        else:
            result["error"] = f"Unknown provider: {provider}. Use 'batch_skip_tracing' or 'skip_genie'."

    except ImportError:
        result["error"] = "requests library not installed. Add 'requests' to requirements.txt"
    except Exception as e:
        result["error"] = str(e)

    return result


def bulk_skip_trace(lead_ids: list, provider: str = "batch_skip_tracing") -> dict:
    """Skip trace multiple leads. Returns summary dict."""
    results = {"success": 0, "failed": 0, "errors": []}
    for lid in lead_ids:
        r = skip_trace_lead(lid, provider)
        if r["success"]:
            results["success"] += 1
        else:
            results["failed"] += 1
            if r["error"]:
                results["errors"].append(f"Lead {lid}: {r['error']}")
    return results


# ================================================================
# UPGRADE 4: MAP / GEO DATA
# ================================================================

def get_leads_with_coords(filters: dict = None) -> list:
    """
    Returns leads with lat/lon for map plotting.
    Geocoding is done on-the-fly using nominatim (free, no API key).
    For production, swap with Google Maps Geocoding API.
    """
    try:
        query = """
            SELECT
                id, street_address, city,
                state, property_state,
                zip_code, owner_name, phone_numbers,
                est_value, est_equity_pct, motivation_score,
                stage, tags,
                lat, lon
            FROM properties
            WHERE lat IS NOT NULL AND lon IS NOT NULL
        """
        params = []

        if filters:
            state = filters.get("state")
            if state and state != "All States":
                query += " AND (state = %s OR property_state = %s)"
                params += [state, state]
            min_score = filters.get("min_score")
            if min_score:
                query += " AND motivation_score >= %s"
                params.append(min_score)
            stage = filters.get("stage")
            if stage and stage != "All":
                query += " AND stage = %s"
                params.append(stage)

        query += " ORDER BY motivation_score DESC NULLS LAST LIMIT 2000"
        rows = execute_query(query, params or None, fetch=True)
        return [dict(r) for r in rows] if rows else []
    except Exception as e:
        print(f"get_leads_with_coords error: {e}")
        return []


def ensure_lat_lon_columns():
    """Add lat/lon columns to properties table if missing."""
    try:
        execute_query("ALTER TABLE properties ADD COLUMN IF NOT EXISTS lat DOUBLE PRECISION")
        execute_query("ALTER TABLE properties ADD COLUMN IF NOT EXISTS lon DOUBLE PRECISION")
        return True
    except Exception as e:
        print(f"ensure_lat_lon_columns error: {e}")
        return False


def geocode_lead(lead_id: int) -> bool:
    """
    Geocode a single lead using Nominatim (OpenStreetMap, free).
    For production volume use Google Maps API instead.
    """
    try:
        import requests, time
        lead = execute_query("SELECT * FROM properties WHERE id = %s", (lead_id,), fetch=True)
        if not lead:
            return False
        lead = dict(lead[0])
        address = f"{lead.get('street_address','')}, {lead.get('city','')}, {lead.get('state') or lead.get('property_state','')}, {lead.get('zip_code','')}"
        resp = requests.get(
            "https://nominatim.openstreetmap.org/search",
            params={"q": address, "format": "json", "limit": 1},
            headers={"User-Agent": "REEnginePro/1.0"},
            timeout=10
        )
        data = resp.json()
        if data:
            lat = float(data[0]["lat"])
            lon = float(data[0]["lon"])
            execute_query("UPDATE properties SET lat = %s, lon = %s WHERE id = %s", (lat, lon, lead_id))
            time.sleep(1)  # Nominatim rate limit: 1 req/sec
            return True
    except Exception as e:
        print(f"geocode_lead error: {e}")
    return False


def batch_geocode(limit: int = 100, state_filter: str = None) -> int:
    """Geocode up to `limit` leads that have no lat/lon yet."""
    try:
        ensure_lat_lon_columns()
        query = "SELECT id FROM properties WHERE (lat IS NULL OR lon IS NULL)"
        params = []
        if state_filter:
            query += " AND (state = %s OR property_state = %s)"
            params += [state_filter, state_filter]
        query += f" LIMIT {int(limit)}"
        leads = execute_query(query, params or None, fetch=True)
        if not leads:
            return 0
        count = 0
        for row in leads:
            if geocode_lead(row["id"]):
                count += 1
        return count
    except Exception as e:
        print(f"batch_geocode error: {e}")
        return 0


# ================================================================
# UPGRADE 5: VIEW-LEVEL KPI AGGREGATES
# ================================================================

def get_view_kpis(lead_ids: list) -> dict:
    """
    Given a list of property IDs (current search results),
    returns KPI aggregates: total equity, avg motivation score, etc.
    """
    if not lead_ids:
        return {"total_equity": 0, "avg_score": 0, "avg_value": 0, "vacant_count": 0, "absentee_count": 0}
    try:
        placeholders = ",".join(["%s"] * len(lead_ids))
        rows = execute_query(f"""
            SELECT
                COALESCE(SUM(est_equity_amt), 0)          AS total_equity,
                COALESCE(AVG(motivation_score), 0)        AS avg_score,
                COALESCE(AVG(est_value), 0)               AS avg_value,
                COUNT(*) FILTER (WHERE vacant = TRUE OR occupancy_status ILIKE '%vacant%') AS vacant_count,
                COUNT(*) FILTER (WHERE is_absentee = TRUE OR
                    (mailing_state IS NOT NULL AND mailing_state != state
                     AND mailing_state != property_state)) AS absentee_count
            FROM properties
            WHERE id IN ({placeholders})
        """, lead_ids, fetch=True)
        if rows:
            r = dict(rows[0])
            return {
                "total_equity":   float(r.get("total_equity") or 0),
                "avg_score":      round(float(r.get("avg_score") or 0), 1),
                "avg_value":      float(r.get("avg_value") or 0),
                "vacant_count":   int(r.get("vacant_count") or 0),
                "absentee_count": int(r.get("absentee_count") or 0),
            }
    except Exception as e:
        print(f"get_view_kpis error: {e}")
    return {"total_equity": 0, "avg_score": 0, "avg_value": 0, "vacant_count": 0, "absentee_count": 0}
