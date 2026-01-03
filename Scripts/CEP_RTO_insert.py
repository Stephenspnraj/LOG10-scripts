import os
import time
from sqlalchemy import create_engine, text, bindparam
import pandas as pd
from datetime import datetime, timedelta

CSV_FILE = os.path.join(os.getenv("WORKSPACE", "."), "CEP_insert.csv")
if not os.path.exists(CSV_FILE):
    raise FileNotFoundError(f"CSV file not found at: {CSV_FILE}")
BATCH_SIZE = 100      

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "log10-replica-replica.cco3osxqlq4g.ap-south-1.rds.amazonaws.com"),
    "user": os.getenv("DB_USER", "log10_scripts"),
    "password": os.getenv("DB_PASSWORD", "D2lx7Wz0Wm9ISAY-Vp1gxKDTzLRRl5k1m"),
    "database": os.getenv("DB_NAME", "loadshare"),
    "port": int(os.getenv("DB_PORT", 3306)),
}

engine = create_engine(
    f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@"
    f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}",
    pool_pre_ping=True,
    pool_recycle=3600,
    future=True,
    echo=False,
)

# ---------- SETUP ----------

df = pd.read_csv(CSV_FILE)

if "waybill_no" not in df.columns:
    raise ValueError("CSV must have a column named 'waybill_no'")

# Drop NaNs, cast to string, and deduplicate
waybill_numbers = (
    df["waybill_no"]
    .dropna()
    .astype(str)
    .drop_duplicates()
    .tolist()
)

print(f"Total unique waybills in CSV: {len(waybill_numbers)}")

# ------------ SQL TEMPLATES ------------
select_forward_batch_sql = text("""
SELECT
    ce1.waybill_no,
    ce1.location_id,
    ce1.`index`,
    ce1.flow_type,
    ce1.created_at,
    ce1.updated_at,
    ce1.is_client_path,
    ce1.location_type,
    ce1.image_flag
FROM consignment_expected_path AS ce1
WHERE ce1.waybill_no IN :waybills
  AND ce1.is_client_path = 1
  AND ce1.flow_type = 'FORWARD'
  AND NOT EXISTS (
        SELECT 1
        FROM consignment_expected_path AS ce2
        WHERE ce2.waybill_no = ce1.waybill_no
          AND ce2.is_client_path = 1
          AND ce2.flow_type = 'RTO'
  )
ORDER BY ce1.waybill_no, ce1.`index` ASC
""").bindparams(bindparam("waybills", expanding=True))

insert_rto_sql = text("""
INSERT INTO consignment_expected_path
(waybill_no, location_id, `index`, flow_type,
 created_at, updated_at, is_client_path, location_type, image_flag)
VALUES
(:waybill_no, :location_id, :index, :flow_type,
 :created_at, :updated_at, :is_client_path, :location_type, :image_flag)
""")

total_inserted = 0

with engine.connect() as conn:
    for batch_start in range(0, len(waybill_numbers), BATCH_SIZE):
        batch = waybill_numbers[batch_start:batch_start + BATCH_SIZE]
        print(f"\n--- Batch {batch_start // BATCH_SIZE + 1} "
              f"({len(batch)} waybills) ---")

        trans = conn.begin()
        try:
            # ✅ pass list, not tuple
            rows = conn.execute(
                select_forward_batch_sql,
                {"waybills": batch}
            ).mappings().all()

            if not rows:
                print("No FORWARD paths (or RTO already present) in this batch, skipping.")
                trans.commit()
                continue

            grouped = {}
            for row in rows:
                grouped.setdefault(row["waybill_no"], []).append(row)

            now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            rto_rows = []

            for wb, path_rows in grouped.items():
                reversed_rows = list(reversed(path_rows))
                for new_index, row in enumerate(reversed_rows):
                    rto_rows.append({
                        "waybill_no": row["waybill_no"],
                        "location_id": row["location_id"],
                        "index": new_index,
                        "flow_type": "RTO",
                        "created_at": now_str,
                        "updated_at": now_str,
                        "is_client_path": row["is_client_path"],
                        "location_type": (row["location_type"] or "").upper(),
                        "image_flag": row["image_flag"],
                    })

            conn.execute(insert_rto_sql, rto_rows)
            total_inserted += len(rto_rows)
            trans.commit()

            print(f"Inserted {len(rto_rows)} RTO rows for this batch.")

        except Exception as e:
            trans.rollback()
            print(f"ERROR in batch: {e}")

        time.sleep(0.5)

print(f"\nDone. Total RTO rows inserted: {total_inserted}")
