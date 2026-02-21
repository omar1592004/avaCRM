# BatchLeads-Style Roadmap — RE Engine Pro (Streamlit)

This doc aligns your app with BatchLeads.io: what you have, what’s fixed, and what to add next. **Stack:** Streamlit + `core.py` (PostgreSQL). Later you can turn it into a desktop/mobile app.

---

## Fixes applied

1. **`core.py` – missing `source`**  
   Bulk import didn’t set `source` when “List Source Name” was empty, causing `KeyError`.  
   **Fix:** `payload.get('source', 'Import')` so a default is always used.

2. **State column name**  
   App used `property_state`; `core.py` uses `state`.  
   **Fix:** App now detects schema and uses either `state` or `property_state` for the state dropdown and filters.

3. **Bulk import optional fields not saved**  
   Mapping beds, baths, property type, occupancy, est value, last sale price did nothing.  
   **Fix:** `stack_lead()` in `core.py` now accepts and writes: `property_type`, `beds`, `baths`, `occupancy_status`, `est_value`, `last_sale_price` on both insert and update. Your `properties` table must have these columns (add via migration if needed).

---

## What you already have (BatchLeads-style)

| Feature | Status |
|--------|--------|
| Lead search with filters (type, beds, baths, occupancy, APN, owner, financials, etc.) | Done |
| State-level market filter | Done |
| Bulk CSV import with column mapping | Done |
| Stack leads (upsert by address+city+state, bump motivation score) | Done |
| Batch actions: Update, Tag, Export, Delete | Done |
| Pipeline tab | Placeholder |
| Tag Manager tab | Placeholder |

---

## Schema expectations (for filters + import)

Your `properties` table should have at least:

- **Identity:** `id`, `street_address`, `city`, `state` (or `property_state`), `zip_code`, `apn`
- **Property:** `property_type`, `beds`, `baths`, `occupancy_status`
- **Owner:** `owner_name`, `owner_type`, `is_absentee`, `years_owned`, `phone_numbers`
- **Financial:** `est_value`, `est_equity_amt`, `est_equity_pct`, `assessed_total`, `last_sale_price`, `last_sale_date`, `has_private_loan`, `is_cash_buyer`
- **Tax:** `tax_delinquent_year`
- **CRM:** `motivation_score`, `stage`, `notes`, `tags`, `last_list_source`

If any column is missing, add it (e.g. `ALTER TABLE properties ADD COLUMN ...`) or temporarily remove that filter/field from the UI.

---

## Missing features (BatchLeads-like) — in order

### Phase 1 – Core experience

1. **Pipeline tab**  
   - Board/view by `stage`: New → Contacted → Negotiating → Closed / Lost.  
   - Drag-and-drop is hard in Streamlit; use selectbox + “Move to stage” or a simple table with stage filter and “Update stage” button.

2. **Tag Manager tab**  
   - List all tags used in `properties.tags` (e.g. split by comma, count).  
   - Rename/merge tags (e.g. “hot-lead” → “hot”).  
   - Optional: bulk add/remove tag from selected leads.

3. **Lead scoring (BatchRankAI-style)**  
   - Add a numeric `lead_score` (or reuse `motivation_score` with a formula).  
   - Formula can use: equity %, absentee, years owned, tax delinquent, last sale date, etc.  
   - Compute on save/import and show in Lead Engine results; sort by score.

### Phase 2 – Contacts & outreach

4. **Skip tracing placeholder**  
   - Add columns: `owner_email`, `owner_phone_2`, etc.  
   - UI: “Skip trace” button per lead or batch → for now just a placeholder that marks “requested” or calls a stub; real integration (e.g. BatchLeads API or another provider) later.

5. **SMS / Email placeholders**  
   - Tables: `campaigns`, `campaign_sends` (lead_id, channel, sent_at, status).  
   - UI: “Send SMS” / “Send email” from lead row or batch → log to DB and optionally call a stub; wire real provider later.

6. **Direct mail placeholder**  
   - Same idea: campaign type “direct_mail”, store address + status; real printing/mail API later.

### Phase 3 – Data & intelligence

7. **Parcel / property insights**  
   - If you have an external data source (e.g. county/parcel API), add a “Refresh data” or “Load details” that fetches and stores more fields.  
   - Otherwise, show “Parcel data: not configured” and a link to docs.

8. **Simple “Reia-style” deal math**  
   - Per-property panel or modal: ARV, repair estimate, max offer, fee.  
   - Store in `properties` or a `deal_analysis` table and show in Pipeline/Lead Engine.

### Phase 4 – Scale & app

9. **Driving for dollars**  
   - List/export by route (e.g. map polygon or list of streets) so you can use it in a mobile app later.  
   - Streamlit: filter by city/zip/address prefix + export for the day’s route.

10. **Turn Streamlit into an app**  
    - Package with Streamlit’s run script; later use PyInstaller/Electron or a PWA so it feels like “one app” (desktop/mobile).

---

## Suggested next steps

1. **Run the app**  
   - Confirm state filter and bulk import work with your DB.  
   - If you get “column does not exist”, add the column or temporarily hide that filter/option.

2. **Implement Pipeline tab**  
   - Query by `stage`, show counts, and “Update stage” for selected leads (reuse your existing batch-update pattern).

3. **Implement Tag Manager**  
   - Parse `tags`, show unique tags + counts, and “Rename tag” (UPDATE … SET tags = replace(tags, old, new)).

4. **Add lead_score**  
   - Add column; compute in `core.py` (or in app after search) and display/sort in Lead Engine.

If you tell me which of these you want first (e.g. “Pipeline tab” or “Tag Manager”), I can outline the exact Streamlit + `core.py` changes step by step.
