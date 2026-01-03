import csv
import requests
import mysql.connector
from mysql.connector import Error

API_URL = "https://meesho-api.loadshare.net/api/v1/partner/upsert"
API_KEY = "5d1d27f37ff3386a0120e2db9fd768e20663f13b"

HEADERS = {
    "X-API-KEY": API_KEY,
    "Content-Type": "application/json"
}

CSV_FILE = "/home/ubuntu/workspace/support/log10/Regular_tasks/Log10 - CSV based User Onboarding/user_input.csv"
BATCH_SIZE = 100


def chunker(iterable, size):
    batch = []
    for item in iterable:
        batch.append(item)
        if len(batch) == size:
            yield batch
            batch = []
    if batch:
        yield batch


def connect_mysql():
    return mysql.connector.connect(
        host="log10-replica-replica.cco3osxqlq4g.ap-south-1.rds.amazonaws.com",
        user="log10_scripts",
        password="D2lx7Wz0Wm9ISAY-Vp1gxKDTzLRRl5k1m",
        database="loadshare",
        port=3306,
        autocommit=False   # transaction control
    )


def main():
    with open(CSV_FILE, newline='', encoding='utf-8') as f:
        reader = list(csv.DictReader(f))

    conn = connect_mysql()
    cursor = conn.cursor()

    batch_number = 1
    for batch in chunker(reader, BATCH_SIZE):
        print(f"\n🟪 ===== Processing Batch {batch_number} ({len(batch)} rows) =====")

        for row in batch:
            payload = {
                "partner_id": row.get("partner_id"),
                "partner_name": row.get("partner_name"),
                "hub_code": row.get("hub_code"),
                "mobile_number": row.get("mobile_number"),
                "partner_type": "LM_PILOT",
                "trigger": row.get("trigger"),
            }

            try:
                res = requests.post(API_URL, headers=HEADERS, json=payload)
                print(f"  🔵 API → {row.get('mobile_number')} : {res.status_code}")
            except Exception as e:
                print(f"  🔴 API Error ({row.get('mobile_number')}): {e}")

        print(f"🟩 Running SQL updates for Batch {batch_number}...")

        try:
            user_updates = 0
            role_mapping_updates = 0

            for row in batch:
                mobile = row.get("mobile_number")
                role_raw = row.get("role", "").strip()
                role_lower = role_raw.lower()

                if role_lower not in ("admin", "branch"):
                    print(f"   ⚪ Skipping SQL for {mobile} (role = {role_raw})")
                    continue

                role_upper = role_raw.upper()

                sql_user = """
                    UPDATE users
                    SET user_level = %s
                    WHERE contact_number = %s;
                """
                cursor.execute(sql_user, (role_upper, mobile))
                user_updates += cursor.rowcount

                role_title = role_raw.capitalize()
                sql_role_mapping = """
                    UPDATE user_location_role_mapping ulrm
                    SET ulrm.role_id = (
                        SELECT r.id
                        FROM roles r
                        WHERE r.partner_id = (
                            SELECT u.entity_id FROM users u WHERE u.contact_number = %s
                        )
                        AND r.is_active = 1
                        AND r.name = %s
                    )
                    WHERE ulrm.is_active = 1
                    AND ulrm.user_id = (
                        SELECT id FROM users WHERE contact_number = %s
                    );
                """
                cursor.execute(sql_role_mapping, (mobile, role_title, mobile))
                role_mapping_updates += cursor.rowcount

            conn.commit()

            print(f"🟩 SQL committed for this batch.")
            print(f"   🔹 Users updated: {user_updates}")
            print(f"   🔹 Role mappings updated: {role_mapping_updates}\n")

        except Error as e:
            conn.rollback()
            print(f"🔴 SQL Error → Batch rolled back: {e}")

        batch_number += 1

    cursor.close()
    conn.close()
    print("\n✨ Done! All batches processed.\n")


if __name__ == "__main__":
    main()
