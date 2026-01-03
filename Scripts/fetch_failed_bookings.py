import pymysql
import pandas as pd
import sys
from datetime import datetime, timedelta

# --- DB Config ---
DB_CONFIG = {
    "host": "log-10-replica-backup.cco3osxqlq4g.ap-south-1.rds.amazonaws.com",    
    "port": 3306,           
    "user": "meesho",
    "password": "dYDxdwV*qf6rDcXiWFkCCcVH",
    "db": "loadshare",
    "charset": "utf8mb4",
    "cursorclass": pymysql.cursors.DictCursor
}

# --- Query Template ---
QUERY_TEMPLATE = """
SELECT
    eal.created_at,
    eal.waybill_no,
    eal.request -> '$.auto_dml' AS autoDMLFlag,
    REPLACE(eal.response -> '$.response.data[0].exceptionMessage', '"', '') AS Error
FROM external_api_logs eal
WHERE eal.created_at BETWEEN %s AND %s
  AND JSON_VALID(request) = 1
  AND eal.response -> '$.response.data[0].isProcessed' <> true
GROUP BY eal.waybill_no, autoDMLFlag, Error
HAVING Error NOT LIKE '%%Booking already processed%%'
ORDER BY eal.created_at;
"""

def fetch_failed_waybills(start_date, end_date, batch_hours=2, output_file="failed_waybills.csv"):
    """Fetch failed waybills in batches and save to CSV"""
    conn = pymysql.connect(**DB_CONFIG)
    all_results = []

    try:
        with conn.cursor() as cursor:
            current_start = start_date
            while current_start < end_date:
                current_end = min(current_start + timedelta(hours=batch_hours), end_date)

                print(f"Fetching from {current_start} to {current_end} ...")
                cursor.execute(QUERY_TEMPLATE, (current_start, current_end))
                batch_result = cursor.fetchall()

                if batch_result:
                    all_results.extend(batch_result)

                current_start = current_end

    finally:
        conn.close()

    # Write to CSV
    if all_results:
        df = pd.DataFrame(all_results)
        df.to_csv(output_file, index=False)
        print(f"✅ Data written to {output_file} ({len(df)} rows)")
    else:
        print("⚠️ No failed waybills found for given range.")


if __name__ == "__main__":
    # --- CLI Arguments ---
    if len(sys.argv) != 3:
        print("Usage: python failed_bookings.py <start_date: YYYY-MM-DD> <end_date: YYYY-MM-DD>")
        sys.exit(1)

    start_date_str = sys.argv[1]
    end_date_str = sys.argv[2]

    # Convert input to datetime ranges
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d") + timedelta(days=1) - timedelta(seconds=1)

    # Auto name output file with range
    # output_file = f"failed_waybills_{start_date_str}_to_{end_date_str}.csv"
    output_file = "failed_waybills.csv"

    # Run extraction
    fetch_failed_waybills(start_date, end_date, batch_hours=2, output_file=output_file)
