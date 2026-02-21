import psycopg2
from psycopg2.extras import RealDictCursor

DB_CONFIG = {
    "dbname": "re_engine",
    "user": "postgres",
    "password": "1592004", 
    "host": "localhost",
    "port": "5432"
}

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

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
    """Get the schema of the properties table"""
    query = """
    SELECT column_name, data_type, is_nullable
    FROM information_schema.columns 
    WHERE table_name = 'properties'
    ORDER BY ordinal_position;
    """
    return execute_query(query, fetch=True)

def _properties_columns():
    """Return set of column names for properties table (cached per call)."""
    schema = get_table_schema()
    return {r["column_name"] for r in schema} if schema else set()


def stack_lead(payload):
    """
    Upsert a property and increment motivation score.
    Returns dict with action status.
    Only includes optional columns (county, mailing_*) if they exist in the table.
    """
    try:
        cols = _properties_columns()
        # Handle owner name - combine first and last if both exist
        owner_name = None
        if payload.get('owner_first') and payload.get('owner_last'):
            # Combine first and last name
            owner_name = f"{payload['owner_first']} {payload['owner_last']}".strip()
        elif payload.get('owner_first'):
            owner_name = payload['owner_first']
        elif payload.get('owner_last'):
            owner_name = payload['owner_last']
        elif payload.get('owner'):
            owner_name = payload['owner']
        
        # First, check if property exists using address + city + state
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
            # UPDATE EXISTING PROPERTY
            update_parts = []
            update_params = []
            
            # Always update these
            update_parts.append("motivation_score = COALESCE(motivation_score, 0) + 1")
            update_parts.append("last_list_source = %s")
            update_params.append(payload.get('source', 'Import'))
            
            # Add optional fields if provided
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
            
            # New property detail fields (check schema)
            optional_fields = [
                ('property_use', 'property_use'), ('land_use', 'land_use'), ('subdivision', 'subdivision'),
                ('legal_description', 'legal_description'), ('living_sqft', 'living_sqft'), ('lot_acres', 'lot_acres'),
                ('lot_sqft', 'lot_sqft'), ('year_built', 'year_built'), ('stories', 'stories'),
                ('units_count', 'units_count'), ('fireplaces', 'fireplaces'), ('garage_type', 'garage_type'),
                ('garage_sqft', 'garage_sqft'), ('carport', 'carport'), ('carport_area', 'carport_area'),
                ('ac_type', 'ac_type'), ('heating_type', 'heating_type'), ('ownership_length_months', 'ownership_length_months'),
                ('owner_type', 'owner_type'), ('owner_occupied', 'owner_occupied'), ('vacant', 'vacant')
            ]
            for payload_key, col_name in optional_fields:
                if payload.get(payload_key) is not None and col_name in cols:
                    update_parts.append(f"{col_name} = %s")
                    update_params.append(payload[payload_key])
            
            # Build the update query
            update_query = f"""
            UPDATE properties 
            SET {', '.join(update_parts)}
            WHERE id = %s
            """
            update_params.append(existing[0]['id'])
            
            execute_query(update_query, update_params)
            return {"action": "updated", "id": existing[0]['id']}
            
        else:
            # INSERT NEW PROPERTY
            insert_columns = []
            insert_placeholders = []
            insert_params = []
            
            # Required fields
            insert_columns.append("street_address")
            insert_placeholders.append("%s")
            insert_params.append(payload['address'])
            
            insert_columns.append("city")
            insert_placeholders.append("%s")
            insert_params.append(payload['city'])
            
            insert_columns.append("state")
            insert_placeholders.append("%s")
            insert_params.append(payload['state'])
            
            insert_columns.append("motivation_score")
            insert_placeholders.append("1")
            
            insert_columns.append("last_list_source")
            insert_placeholders.append("%s")
            insert_params.append(payload.get('source', 'Import'))
            
            # Add optional fields if provided
            if payload.get('zip'):
                insert_columns.append("zip_code")
                insert_placeholders.append("%s")
                insert_params.append(payload['zip'])
            
            if owner_name:
                insert_columns.append("owner_name")
                insert_placeholders.append("%s")
                insert_params.append(owner_name)
            
            if payload.get('apn'):
                insert_columns.append("apn")
                insert_placeholders.append("%s")
                insert_params.append(payload['apn'])
            
            if payload.get('phone_numbers'):
                insert_columns.append("phone_numbers")
                insert_placeholders.append("%s")
                insert_params.append(payload['phone_numbers'])
            if payload.get('property_type') is not None:
                insert_columns.append("property_type")
                insert_placeholders.append("%s")
                insert_params.append(payload['property_type'])
            if payload.get('beds') is not None:
                insert_columns.append("beds")
                insert_placeholders.append("%s")
                insert_params.append(payload['beds'])
            if payload.get('baths') is not None:
                insert_columns.append("baths")
                insert_placeholders.append("%s")
                insert_params.append(payload['baths'])
            if payload.get('occupancy_status') is not None:
                insert_columns.append("occupancy_status")
                insert_placeholders.append("%s")
                insert_params.append(payload['occupancy_status'])
            if payload.get('est_value') is not None:
                insert_columns.append("est_value")
                insert_placeholders.append("%s")
                insert_params.append(payload['est_value'])
            if payload.get('last_sale_price') is not None:
                insert_columns.append("last_sale_price")
                insert_placeholders.append("%s")
                insert_params.append(payload['last_sale_price'])
            if payload.get('county') is not None and 'county' in cols:
                insert_columns.append("county")
                insert_placeholders.append("%s")
                insert_params.append(payload['county'])
            if payload.get('mailing_address') is not None and 'mailing_address' in cols:
                insert_columns.append("mailing_address")
                insert_placeholders.append("%s")
                insert_params.append(payload['mailing_address'])
            if payload.get('mailing_city') is not None and 'mailing_city' in cols:
                insert_columns.append("mailing_city")
                insert_placeholders.append("%s")
                insert_params.append(payload['mailing_city'])
            if payload.get('mailing_state') is not None and 'mailing_state' in cols:
                insert_columns.append("mailing_state")
                insert_placeholders.append("%s")
                insert_params.append(payload['mailing_state'])
            if payload.get('mailing_zip') is not None and 'mailing_zip' in cols:
                insert_columns.append("mailing_zip")
                insert_placeholders.append("%s")
                insert_params.append(payload['mailing_zip'])
            
            # New property detail fields (check schema)
            optional_fields = [
                ('property_use', 'property_use'), ('land_use', 'land_use'), ('subdivision', 'subdivision'),
                ('legal_description', 'legal_description'), ('living_sqft', 'living_sqft'), ('lot_acres', 'lot_acres'),
                ('lot_sqft', 'lot_sqft'), ('year_built', 'year_built'), ('stories', 'stories'),
                ('units_count', 'units_count'), ('fireplaces', 'fireplaces'), ('garage_type', 'garage_type'),
                ('garage_sqft', 'garage_sqft'), ('carport', 'carport'), ('carport_area', 'carport_area'),
                ('ac_type', 'ac_type'), ('heating_type', 'heating_type'), ('ownership_length_months', 'ownership_length_months'),
                ('owner_type', 'owner_type'), ('owner_occupied', 'owner_occupied'), ('vacant', 'vacant')
            ]
            for payload_key, col_name in optional_fields:
                if payload.get(payload_key) is not None and col_name in cols:
                    insert_columns.append(col_name)
                    insert_placeholders.append("%s")
                    insert_params.append(payload[payload_key])
            
            # Build the insert query
            insert_query = f"""
            INSERT INTO properties (
                {', '.join(insert_columns)}
            ) VALUES (
                {', '.join(insert_placeholders)}
            )
            """
            
            execute_query(insert_query, insert_params)
            return {"action": "inserted"}
            
    except Exception as e:
        print(f"Error in stack_lead: {str(e)}")
        print(f"Payload: {payload}")
        raise e


def get_pipeline_counts():
    """Return list of dicts: stage (or 'Unset'), count. Requires 'stage' column."""
    q = """
    SELECT COALESCE(NULLIF(TRIM(stage), ''), 'Unset') AS stage, COUNT(*) AS cnt
    FROM properties
    GROUP BY COALESCE(NULLIF(TRIM(stage), ''), 'Unset')
    ORDER BY cnt DESC
    """
    return execute_query(q, fetch=True)


def get_leads_by_stage(stage_filter=None):
    """Return all properties, optionally filtered by stage. stage_filter 'Unset' = NULL/empty."""
    if stage_filter is None or stage_filter == "All":
        return execute_query("SELECT * FROM properties ORDER BY id", fetch=True)
    if stage_filter == "Unset":
        return execute_query(
            "SELECT * FROM properties WHERE (stage IS NULL OR TRIM(stage) = '') ORDER BY id",
            fetch=True
        )
    return execute_query(
        "SELECT * FROM properties WHERE TRIM(stage) = %s ORDER BY id",
        (stage_filter,),
        fetch=True
    )


def update_stage(lead_ids, new_stage):
    """Set stage for given list of property ids."""
    if not lead_ids:
        return 0
    placeholders = ",".join(["%s"] * len(lead_ids))
    q = f"UPDATE properties SET stage = %s WHERE id IN ({placeholders})"
    execute_query(q, [new_stage] + list(lead_ids))
    return len(lead_ids)


def get_all_tags_with_counts():
    """Return list of dicts: tag_name, cnt. Uses tags column (comma-separated)."""
    rows = execute_query("SELECT id, tags FROM properties WHERE tags IS NOT NULL AND TRIM(tags) != ''", fetch=True)
    from collections import Counter
    counter = Counter()
    for r in rows:
        for t in (r["tags"] or "").split(","):
            t = t.strip()
            if t:
                counter[t] += 1
    return [{"tag_name": k, "cnt": v} for k, v in counter.most_common()]


def rename_tag(old_name, new_name):
    """Replace old tag with new name in all properties. Case-insensitive match on whole tag."""
    rows = execute_query("SELECT id, tags FROM properties WHERE tags IS NOT NULL AND TRIM(tags) != ''", fetch=True)
    updated = 0
    for r in rows:
        tags = [t.strip() for t in (r["tags"] or "").split(",") if t.strip()]
        new_tags = [new_name if t.lower() == old_name.lower() else t for t in tags]
        if new_tags != tags:
            execute_query("UPDATE properties SET tags = %s WHERE id = %s", (", ".join(new_tags), r["id"]))
            updated += 1
    return updated


def get_leads_by_tag(tag_name):
    """Return list of property dicts that have the given tag (comma-separated tags, whole-tag match)."""
    if not tag_name or not str(tag_name).strip():
        return []
    q = """
        SELECT * FROM properties
        WHERE LOWER(TRIM(%s)) IN (
            SELECT LOWER(TRIM(t)) FROM unnest(string_to_array(COALESCE(tags, ''), ',')) AS t
            WHERE TRIM(t) != ''
        )
        ORDER BY street_address
    """
    return execute_query(q, (str(tag_name).strip(),), fetch=True) or []


def remove_tag_from_all(tag_name):
    """Remove tag from every property that has it."""
    rows = execute_query("SELECT id, tags FROM properties WHERE tags IS NOT NULL AND TRIM(tags) != ''", fetch=True)
    updated = 0
    for r in rows:
        tags = [t.strip() for t in (r["tags"] or "").split(",") if t.strip()]
        new_tags = [t for t in tags if t.lower() != tag_name.lower()]
        if len(new_tags) < len(tags):
            execute_query("UPDATE properties SET tags = %s WHERE id = %s", (", ".join(new_tags) if new_tags else None, r["id"]))
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
    """Save a search. filters_json is a JSON string of the filter dict."""
    _ensure_saved_searches_table()
    execute_query(
        "INSERT INTO saved_searches (name, filters_json) VALUES (%s, %s)",
        (name.strip(), filters_json)
    )


def list_saved_searches():
    """Return list of {id, name, created_at}."""
    _ensure_saved_searches_table()
    rows = execute_query(
        "SELECT id, name, created_at FROM saved_searches ORDER BY created_at DESC",
        fetch=True
    )
    return rows or []


def get_saved_search(search_id):
    """Return {name, filters_json} or None."""
    rows = execute_query(
        "SELECT name, filters_json FROM saved_searches WHERE id = %s",
        (search_id,),
        fetch=True
    )
    return rows[0] if rows else None


def delete_saved_search(search_id):
    execute_query("DELETE FROM saved_searches WHERE id = %s", (search_id,))


# ---------- Dashboard ----------
def get_dashboard_stats():
    """Return total count, list of {state, cnt}, list of {stage, cnt} for dashboard."""
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


# ---------- Uploaded files / list sources ----------
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
    """Record an uploaded file/list. name = list source name or filename."""
    _ensure_uploaded_lists_table()
    execute_query(
        "INSERT INTO uploaded_lists (name, filename, status) VALUES (%s, %s, 'new')",
        (name.strip() or filename, filename)
    )


def list_uploaded_lists():
    """Return list of {id, name, filename, uploaded_at, status} with lead_count (from properties.last_list_source)."""
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
                (r["name"],),
                fetch=True
            )
            if cnt:
                r["lead_count"] = cnt[0]["c"]
        except Exception:
            pass
    return rows


def update_uploaded_list_status(list_id, status):
    if status not in UPLOAD_STATUSES:
        raise ValueError(f"Status must be one of {UPLOAD_STATUSES}")
    execute_query(
        "UPDATE uploaded_lists SET status = %s WHERE id = %s",
        (status, list_id)
    )


def delete_uploaded_lists(ids):
    """Delete uploaded_lists rows by id. ids = list of int."""
    if not ids:
        return
    placeholders = ",".join(["%s"] * len(ids))
    execute_query(f"DELETE FROM uploaded_lists WHERE id IN ({placeholders})", list(ids))


# ---------- Clear data (by state or all) ----------
def count_properties(state_filter=None):
    """Return number of properties. state_filter=None or 'All' = all; else filter by that state (state or property_state column)."""
    if state_filter is None or state_filter == "All" or state_filter == "":
        try:
            r = execute_query("SELECT COUNT(*) AS c FROM properties", fetch=True)
            return r[0]["c"] if r and len(r) > 0 else 0
        except Exception as e:
            print(f"Error counting all properties: {e}")
            return 0
    # Try state column first, then property_state
    for col in ("state", "property_state"):
        try:
            r = execute_query(f"SELECT COUNT(*) AS c FROM properties WHERE {col} = %s", (state_filter,), fetch=True)
            if r and len(r) > 0:
                return r[0]["c"]
        except Exception:
            if col == "property_state":  # Last attempt failed
                print(f"Error counting properties for state {state_filter}")
            continue
    return 0


def delete_properties(state_filter=None):
    """Delete properties. state_filter=None or 'All' = delete all; else delete where state/property_state = state_filter."""
    if state_filter is None or state_filter == "All" or state_filter == "":
        try:
            execute_query("DELETE FROM properties")
            return
        except Exception as e:
            print(f"Error deleting all properties: {e}")
            raise e
    # Try state column first, then property_state
    for col in ("state", "property_state"):
        try:
            execute_query(f"DELETE FROM properties WHERE {col} = %s", (state_filter,))
            return
        except Exception as e:
            # If first column fails, try next one
            if col == "state":
                continue
            # If both fail, raise the last error
            print(f"Error deleting properties for state {state_filter}: {e}")
            raise e

