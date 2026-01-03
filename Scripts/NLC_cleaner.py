import sys
import pandas as pd
import pymysql

# ======================================================
# MODE HANDLING
# ======================================================
MODE = sys.argv[1].upper() if len(sys.argv) > 1 else "DRYRUN"  # DRYRUN / UPDATE

if MODE not in ("DRYRUN", "UPDATE"):
    raise ValueError("MODE must be DRYRUN or UPDATE")

DRY_RUN = MODE == "DRYRUN"

print(f"\n🚦 RUN MODE: {MODE}")

# DB Connection
conn = pymysql.connect(
    host="log10-replica-replica.cco3osxqlq4g.ap-south-1.rds.amazonaws.com",
    user="log10_scripts",
    password="D2lx7Wz0Wm9ISAY-Vp1gxKDTzLRRl5k1m",
    database="loadshare",
    port=3306
)
cursor = conn.cursor()

# ======================================================
# UPDATE HANDLER
# ======================================================
def run_update(cursor, conn, query, ids, label):
    if not ids:
        print(f"✅ {label}: No records found")
        return

    print(f"\n🔹 {label}")
    print(f"   Rows affected: {len(ids)}")

    if DRY_RUN:
        print("🟡 DRYRUN MODE — UPDATE SKIPPED")
        print(query.strip())
    else:
        cursor.execute(query)
        conn.commit()
        print("🔴 UPDATE EXECUTED")

# ======================================================
# 1️⃣ CONDITION 1 — Multi-SC Cleanup
# ======================================================
condition1_query = """
SELECT diff.deleted_id
FROM next_location_configs nlc
INNER JOIN (
    SELECT 
        location_id, 
        pincode_id,
        MIN(id) AS deleted_id
    FROM next_location_configs
    WHERE entity_type = 'MANIFEST'
      AND is_active = 1
    GROUP BY location_id, pincode_id
    HAVING COUNT(DISTINCT next_location_id) > 1
) diff
  ON nlc.location_id = diff.location_id
  AND nlc.pincode_id = diff.pincode_id
JOIN locations l ON nlc.location_id = l.id
JOIN locations ln ON nlc.next_location_id = ln.id
JOIN locations dl 
  ON dl.pincode_id = nlc.pincode_id
  AND dl.entity_type = 'PARTNER'
  AND dl.status = 1
  AND dl.is_valmo_location = 1
WHERE 
  nlc.entity_type = 'MANIFEST'
  AND nlc.is_active = 1
  AND nlc.audit_log IN ('multi-sc wrong facility','WF_BAGGING_CRON')
ORDER BY 
  nlc.location_id, nlc.pincode_id, nlc.updated_at DESC;
"""

cursor.execute(condition1_query)
condition1_ids = [row[0] for row in cursor.fetchall()]

update1 = f"""
UPDATE loadshare.next_location_configs
SET is_active = 0,
    audit_log = 'Duplicate nlc cleaner job'
WHERE id IN ({','.join(map(str, condition1_ids))});
"""

run_update(cursor, conn, update1, condition1_ids, "Condition1 (Multi-SC Cleanup)")

# ======================================================
# 2️⃣ CONDITION 2 — Reciprocal Mismatch Checker
# ======================================================
duplicate_finder_query = """
SELECT
  nlc.id,
  l.alias AS source,
  ln.alias AS next_location,
  dl.alias AS dest_loc
FROM
  next_location_configs nlc
  INNER JOIN (
      SELECT 
          location_id, 
          pincode_id,
          MIN(id) AS deleted_id
      FROM next_location_configs
      WHERE entity_type = 'MANIFEST'
        AND is_active = 1
      GROUP BY location_id, pincode_id
      HAVING COUNT(DISTINCT next_location_id) > 1
  ) diff
    ON nlc.location_id = diff.location_id
    AND nlc.pincode_id = diff.pincode_id
  JOIN locations l ON nlc.location_id = l.id
  JOIN locations ln ON nlc.next_location_id = ln.id
  JOIN locations dl 
    ON dl.pincode_id = nlc.pincode_id
    AND dl.entity_type = 'PARTNER'
    AND dl.status = 1
    AND dl.is_valmo_location = 1
WHERE 
  nlc.entity_type = 'MANIFEST'
  AND nlc.is_active = 1
ORDER BY 
  nlc.location_id, nlc.pincode_id, nlc.updated_at DESC;
"""

cursor.execute(duplicate_finder_query)
duplicates = cursor.fetchall()

invalid_ids = []
report_rows = []

for path_id, source, next_loc, dest_loc in duplicates:

    if next_loc == dest_loc:
        expected = (next_loc, source, source)
    else:
        expected = (dest_loc, next_loc, source)

    reverse_check_query = """
    SELECT 1
    FROM next_location_configs nlc
    JOIN locations l ON nlc.location_id = l.id
    JOIN locations ln ON nlc.next_location_id = ln.id
    JOIN locations dl 
      ON dl.pincode_id = nlc.pincode_id
      AND dl.entity_type = 'PARTNER'
      AND dl.status = 1
      AND dl.is_valmo_location = 1
    WHERE nlc.entity_type = 'MANIFEST'
      AND nlc.is_active = 1
      AND l.alias = %s
      AND ln.alias = %s
      AND dl.alias = %s
    LIMIT 1;
    """

    cursor.execute(reverse_check_query, expected)
    reverse_exists = cursor.fetchone()

    status = "MATCH" if reverse_exists else "MISMATCH"
    report_rows.append((path_id, source, next_loc, dest_loc, expected, status))

    if not reverse_exists:
        invalid_ids.append(path_id)

# Export report
df = pd.DataFrame(
    report_rows,
    columns=["id", "source", "next_loc", "dest_loc", "expected_reverse", "status"]
)
df.to_excel("duplicate_mismatch_report.xlsx", index=False)
print("\n✅ Report generated: duplicate_mismatch_report.xlsx")

update2 = f"""
UPDATE loadshare.next_location_configs
SET is_active = 0,
    audit_log = 'Duplicate nlc cleaner job'
WHERE id IN ({','.join(map(str, invalid_ids))})
  AND audit_log <> 'do not delete';
"""

run_update(cursor, conn, update2, invalid_ids, "Condition2 (Reciprocal Mismatch Cleanup)")

# ======================================================
# 3️⃣ CONDITION 3 — Manual Cleanup
# ======================================================
manual_lanes = [
    ("FRS", "FRDS", "FRDS")
]

manual_ids = []

for src, nloc, dest in manual_lanes:
    query = """
    SELECT nlc.id
    FROM next_location_configs nlc
    JOIN locations l ON nlc.location_id = l.id
    JOIN locations ln ON nlc.next_location_id = ln.id
    JOIN locations dl 
      ON dl.pincode_id = nlc.pincode_id
      AND dl.entity_type = 'PARTNER'
      AND dl.status = 1
      AND dl.is_valmo_location = 1
    WHERE nlc.entity_type = 'MANIFEST'
      AND nlc.is_active = 1
      AND l.alias = %s
      AND ln.alias = %s
      AND dl.alias = %s;
    """
    cursor.execute(query, (src, nloc, dest))
    manual_ids.extend([row[0] for row in cursor.fetchall()])

update3 = f"""
UPDATE loadshare.next_location_configs
SET is_active = 0,
    audit_log = 'Duplicate nlc cleaner job'
WHERE id IN ({','.join(map(str, manual_ids))});
"""

run_update(cursor, conn, update3, manual_ids, "Condition3 (Manual Overrides)")

# ======================================================
# CLEANUP
# ======================================================
print("\n🏁 Script completed")
print(f"🚦 FINAL MODE: {MODE}")

try:
    if cursor:
        cursor.close()
except Exception:
    pass

try:
    if conn and conn.open:
        conn.close()
except Exception:
    pass



