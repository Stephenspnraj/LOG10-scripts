import pandas as pd
from sqlalchemy import create_engine, text
import time

CSV_FILE = 'input.csv'
DB_URI = "mysql+pymysql://log10_scripts:D2lx7Wz0Wm9ISAY-Vp1gxKDTzLRRl5k1m@log10-replica-replica.cco3osxqlq4g.ap-south-1.rds.amazonaws.com:3306/loadshare"

engine = create_engine(DB_URI)

df = pd.read_csv(CSV_FILE)
df.columns = df.columns.str.strip().str.lower()   # normalize

# Required columns
REQUIRED_COLUMNS = ["manifest_code", "current_location", "destination_location", "next_location"]

missing = [col for col in REQUIRED_COLUMNS if col not in df.columns]
if missing:
    raise ValueError(f"CSV must have columns: {missing}")

with engine.begin() as conn:
    for _, row in df.iterrows():
        manifest_code = row["manifest_code"].strip()
        cur_alias = row["current_location"].strip()
        dest_alias = row["destination_location"].strip()
        next_alias = row["next_location"].strip()

        # ---------- FETCH MANIFEST ----------
        fetch_manifest = text("""
            SELECT id, manifest_status, current_loc_id, destination_loc_id
            FROM manifests
            WHERE manifest_code = :m
        """)
        manifest = conn.execute(fetch_manifest, {"m": manifest_code}).mappings().fetchone()

        if not manifest:
            print(f"❌ Manifest {manifest_code} not found. Skipping.")
            continue

        # Only process when status IS Pending
        if manifest["manifest_status"].strip().upper() != "PENDING":
            print(f"⏸️ Manifest {manifest_code} is in {manifest['manifest_status']} status (not Pending). Skipping.")
            continue

        # ---------- VALIDATE CURRENT + DESTINATION ----------
        fetch_loc_aliases = text("""
            SELECT id, alias
            FROM locations
            WHERE id IN (:cur_id, :dest_id)
        """)
        locs = conn.execute(fetch_loc_aliases, {
            "cur_id": manifest["current_loc_id"],
            "dest_id": manifest["destination_loc_id"]
        }).mappings().all()
        alias_map = {row["id"]: row["alias"] for row in locs}

        db_cur_alias = alias_map.get(manifest["current_loc_id"])
        db_dest_alias = alias_map.get(manifest["destination_loc_id"])

        if db_cur_alias != cur_alias or db_dest_alias != dest_alias:
            print(f"❌ Manifest {manifest_code}: CSV current/destination do not match DB values. Skipping.")
            continue

        # ---------- FETCH NEXT LOCATION CONFIG ----------
        fetch_nlc = text("""
            SELECT nlc.id, l.id AS current_loc_id, ln.id AS next_loc_id
            FROM next_location_configs nlc
            JOIN locations l ON nlc.location_id = l.id
            JOIN locations ln ON nlc.next_location_id = ln.id
            JOIN locations dl ON dl.pincode_id = nlc.pincode_id
                AND dl.entity_type = 'PARTNER'
                AND dl.is_valmo_location = 1
            WHERE nlc.entity_type = 'MANIFEST'
              AND nlc.is_active = 1
              AND l.alias = :cur
              AND dl.alias = :dest
              AND ln.alias = :nxt
            ORDER BY nlc.id DESC
            LIMIT 1
        """)
        nlc = conn.execute(fetch_nlc, {"cur": cur_alias, "dest": dest_alias, "nxt": next_alias}).mappings().fetchone()

        if not nlc:
            print(f"❌ NLC mapping not found for manifest {manifest_code}. Skipping.")
            continue

        # ---------- UPDATE MANIFEST ----------
        update_manifest = text("""
            UPDATE manifests
            SET next_loc_id = :next_id
            WHERE manifest_code = :m
        """)
        conn.execute(update_manifest, {"next_id": nlc["next_loc_id"], "m": manifest_code})
        print(f"✅ Manifest {manifest_code} updated successfully to next location {next_alias}.")
    
    time.sleep(1)
