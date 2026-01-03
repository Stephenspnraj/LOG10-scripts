from datetime import datetime
import pandas as pd
import pytz
import requests
from sqlalchemy import create_engine, text
import sys

# Database configurations
log10_db = {
    "user": "log10_scripts",
    "password": "D2lx7Wz0Wm9ISAY-Vp1gxKDTzLRRl5k1m",
    "host": "log10-replica-replica.cco3osxqlq4g.ap-south-1.rds.amazonaws.com",
    "port": "3306",
    "dbname": "loadshare"
}
titan_db = {
    "user": "prod_zappay_read",
    "password": "7Xc8zapEagQread",
    "host": "prod-zappay-replica-rds.cco3osxqlq4g.ap-south-1.rds.amazonaws.com",
    "port": "3306",
    "dbname": "zappay"
}

# Create database engines
log10_url = f"mysql+mysqlconnector://{log10_db['user']}:{log10_db['password']}@{log10_db['host']}:{log10_db['port']}/{log10_db['dbname']}"
titan_url = f"mysql+mysqlconnector://{titan_db['user']}:{titan_db['password']}@{titan_db['host']}:{titan_db['port']}/{titan_db['dbname']}"
log10_engine = create_engine(log10_url)
titan_engine = create_engine(titan_url)

# Read waybill numbers from CSV file
try:
    df = pd.read_csv('input_waybills.csv')
    if 'waybill_no' not in df.columns:
        raise ValueError("CSV file must contain 'waybill_no' column")
    waybills = df['waybill_no'].tolist()
except Exception as e:
    print(f"Error reading CSV: {str(e)}")
    sys.exit(1)

india_tz = pytz.timezone('Asia/Kolkata')
today = datetime.now(india_tz)
today_midnight = india_tz.localize(datetime(today.year, today.month, today.day))
results = []
total_waybills = len(waybills)

for index, waybill_no in enumerate(waybills, start=1):
    remark = ""
    try:
        print(f"Processing waybill_no({index}/{total_waybills}): {waybill_no}: No issues on this waybill")
        log10_query = text("""
        SELECT
            prd.consignment_id,
            prd.waybill_no,
            prd.entity_code,
            ps.payment_reference_number,
            ps.payment_type,
            ps.total_amount,
            ps.status,
            ps.error_code,
            ps.created_at,
            ps.updated_at,
            c.consignment_status,
            c.updated_at AS consignment_updated_at,
            ptp.deposit_status
        FROM
            consignment_payment_reference_status ps
        JOIN consignment_payment_reference_details prd ON
            ps.payment_reference_number = prd.payment_reference_number
        JOIN consignments c ON prd.consignment_id = c.id
        LEFT JOIN partner_transaction_payable ptp ON ptp.waybill_no = c.waybill_no
        WHERE
            prd.waybill_no = :waybill_no
        """)
        log10_df = pd.read_sql_query(log10_query, log10_engine, params={"waybill_no": waybill_no})

        if log10_df.empty:
            remark = "No data found in log10 query"
            print(f"Processing waybill_no({index}/{total_waybills}): {waybill_no}: {remark}")
            results.append({"waybill_no": waybill_no, "remarks": remark})
            continue

        required_columns = ['status', 'payment_reference_number', 'consignment_status', 'consignment_updated_at', 'deposit_status', 'total_amount', 'consignment_id', 'payment_type']
        missing_columns = [col for col in required_columns if col not in log10_df.columns]
        if missing_columns:
            remark = f"Missing columns in log10_df: {', '.join(missing_columns)}"
            print(f"Processing waybill_no({index}/{total_waybills}): {waybill_no}: {remark}")
            results.append({"waybill_no": waybill_no, "remarks": remark})
            continue

        log10_status = log10_df['status'].iloc[0]
        payment_reference_number = log10_df['payment_reference_number'].iloc[0]
        consignment_status = log10_df['consignment_status'].iloc[0]
        ofd_at = pd.to_datetime(log10_df['consignment_updated_at'].iloc[0])
        if ofd_at.tzinfo is None:
            ofd_at = india_tz.localize(ofd_at)

        titan_query = text("""
        SELECT
            amount AS order_Amount,
            source_link_id AS referenceId,
            transaction_status AS txStatus,
            bank_reference_number AS utrNumber,
            entity_value AS trackingId,
            entity_id AS runsheetId,
            link_status,
            is_deleted,
            payment_at,
            plc.updated_at,
            plc.created_at,
            transaction_id,
            pv.name
        FROM
            zappay.payment_links_cod plc
        JOIN zappay.payment_vendors pv ON plc.payment_vendor_id = pv.id
        WHERE
            source_link_id = :payment_reference_number
        """)
        titan_df = pd.read_sql_query(titan_query, titan_engine, params={"payment_reference_number": payment_reference_number})

        if titan_df.empty:
            remark = "No payment link data found in titan query"
            print(f"Processing waybill_no({index}/{total_waybills}): {waybill_no}: {remark}")
            results.append({"waybill_no": waybill_no, "remarks": remark})
            continue

        required_titan_columns = ['txStatus', 'trackingId', 'runsheetId', 'name']
        missing_titan_columns = [col for col in required_titan_columns if col not in titan_df.columns]
        if missing_titan_columns:
            remark = f"Missing columns in titan_df: {', '.join(missing_titan_columns)}"
            print(f"Processing waybill_no({index}/{total_waybills}): {waybill_no}: {remark}")
            results.append({"waybill_no": waybill_no, "remarks": remark})
            continue

        zappay_status = titan_df['txStatus'].iloc[0]
        prd_query = text("""
        SELECT DISTINCT waybill_no
        FROM consignment_payment_reference_details
        WHERE payment_reference_number = :payment_reference_number
        """)
        prd_df = pd.read_sql_query(prd_query, log10_engine, params={"payment_reference_number": payment_reference_number})

        if zappay_status != "SUCCESS":
            remark = "Case 1: Please contact Meesho"
            print(f"Processing waybill_no({index}/{total_waybills}): {waybill_no}: {remark}")
            results.append({"waybill_no": waybill_no, "remarks": remark})
            continue

        if log10_status == 'INITIATED' and zappay_status == 'SUCCESS' and consignment_status == 'OFD' and ofd_at > today_midnight:
            remark = "Case 2: Sending webhook"
            print(f"Processing waybill_no({index}/{total_waybills}): {waybill_no}: {remark}")
            url = "https://meesho-api.loadshare.net/lm/external/payment/webhook"
            headers = {
                "token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ2ZW5kb3IiOiJIZXJtZXMiLCJlbnYiOiJQcm9kdWN0aW9uIn0.DL6OMhM5DFz9-Mp3b7Iz1cigKpD4iEwi3yuCTsUGmlA",
                "Content-Type": "application/json"
            }
            data = {
                "orderAmount": int(log10_df['total_amount'].iloc[0]),
                "paymentMode": "RIDERAPP_UPI",
                "referenceId": payment_reference_number,
                "txStatus": "SUCCESS",
                "runsheetId": int(titan_df['runsheetId'].iloc[0]),
                "trackingId": [titan_df['trackingId'].iloc[0]],
                "taskId": [int(log10_df['consignment_id'].iloc[0])],
                "txMsg": "",
                "txTime": "",
                "utrNumber": ""
            }
            response = requests.post(url, headers=headers, json=data)
            if response.status_code == 200:
                remark += " - Webhook request successful"
            else:
                remark += f" - Webhook request failed with code {response.status_code}"
            print(f"Processing waybill_no({index}/{total_waybills}): {waybill_no}: {remark}")
            results.append({"waybill_no": waybill_no, "remarks": remark})
            continue

        if log10_status == 'INITIATED' and zappay_status == 'SUCCESS' and consignment_status == 'OFD' and ofd_at < today_midnight:
            remark = "Case 3: Updating status to SUCCESS and inserting transaction"
            print(f"Processing waybill_no({index}/{total_waybills}): {waybill_no}: {remark}")
            update_query = text("""
                UPDATE consignment_payment_reference_status
                SET `status` = 'SUCCESS'
                WHERE payment_reference_number = :payment_reference_number
            """)
            insert_query = text("""
                INSERT INTO card_transactions_details
                    (consignment_id, amount, amount_original, currency_code, payment_mode, payment_vendor, status, external_ref_number, external_ref_number1)
                VALUES
                    (:consignment_id, :amount, :amount_original, :currency_code, :payment_mode, :payment_vendor, :status, :external_ref_number, :external_ref_number1)
            """)
            with log10_engine.begin() as conn:
                conn.execute(update_query, {"payment_reference_number": payment_reference_number})
                conn.execute(insert_query, {
                    "consignment_id": int(log10_df['consignment_id'].iloc[0]),
                    "amount": int(log10_df['total_amount'].iloc[0]),
                    "amount_original": int(log10_df['total_amount'].iloc[0]),
                    "currency_code": 'INR',
                    "payment_mode": 'RIDERAPP_UPI',
                    "payment_vendor": 'RAZORPAY',
                    "status": 'SUCCESS',
                    "external_ref_number": payment_reference_number,
                    "external_ref_number1": payment_reference_number
                })
            results.append({"waybill_no": waybill_no, "remarks": remark})
            continue

        if log10_status == 'INITIATED' and zappay_status == 'SUCCESS' and consignment_status == 'DEL' and log10_df['deposit_status'].iloc[0] == 'PENDING':
            remark = "Case 4: Updating statuses for DEL and PENDING deposit"
            print(f"Processing waybill_no({index}/{total_waybills}): {waybill_no}: {remark}")
            for index_prd, row in prd_df.iterrows():
                wb_no = row['waybill_no']
                update_query = text("""
                    UPDATE consignment_payment_reference_status
                    SET `status` = 'SUCCESS'
                    WHERE payment_reference_number = :payment_reference_number
                """)
                insert_query = text("""
                    INSERT INTO card_transactions_details
                        (consignment_id, amount, amount_original, currency_code, payment_mode, payment_vendor, status, external_ref_number, external_ref_number1)
                    VALUES
                        (:consignment_id, :amount, :amount_original, :currency_code, :payment_mode, :payment_vendor, :status, :external_ref_number, :external_ref_number1)
                """)
                update_ptp = text("""
                    UPDATE partner_transaction_payable
                    SET payment_mode = 'RIDERAPP_UPI',
                        deposit_settlement_id = hand_over_settlement_id,
                        remitted_settlement_id = hand_over_settlement_id,
                        deposit_status = 'APPROVED',
                        remittance_status = 'APPROVED',
                        new_remittance_status = 'APPROVED',
                        pending_deposit_amount = '0.00',
                        pending_remittance_amount = '0.00',
                        deposit_amount_pending_for_request = '0.00',
                        remittance_amount_pending_for_request = '0.00',
                        deposit_amount_for_approval = '0.00',
                        remittance_amount_for_approval = '0.00'
                    WHERE waybill_no = :wb_no
                """)
                cpod_update = text("""
                    UPDATE consignments_pod
                    SET payment_type = 'RIDERAPP_UPI'
                    WHERE waybill_no = :wb_no AND shipment_status = 'DEL'
                """)
                si_update = text("""
                    UPDATE settlement_info
                    SET payment_option = 'RIDERAPP_UPI'
                    WHERE id IN (
                        SELECT hand_over_settlement_id
                        FROM partner_transaction_payable
                        WHERE waybill_no = :wb_no
                    )
                """)
                ct_update = text("""
                    UPDATE consignment_tracking
                    SET sync_status = 'PENDING',
                        data = JSON_SET(data, '$.paymentType', 'RIDERAPP_UPI')
                    WHERE waybill_number = :wb_no AND event_type = 'DELIVERED'
                """)
                with log10_engine.begin() as conn:
                    conn.execute(update_query, {"payment_reference_number": payment_reference_number})
                    conn.execute(insert_query, {
                        "consignment_id": int(log10_df['consignment_id'].iloc[0]),
                        "amount": int(log10_df['total_amount'].iloc[0]),
                        "amount_original": int(log10_df['total_amount'].iloc[0]),
                        "currency_code": 'INR',
                        "payment_mode": log10_df['payment_type'].iloc[0],
                        "payment_vendor": titan_df['name'].iloc[0],
                        "status": zappay_status,
                        "external_ref_number": payment_reference_number,
                        "external_ref_number1": payment_reference_number
                    })
                    conn.execute(update_ptp, {"wb_no": wb_no})
                    conn.execute(cpod_update, {"wb_no": wb_no})
                    conn.execute(si_update, {"wb_no": wb_no})
                    conn.execute(ct_update, {"wb_no": wb_no})
            results.append({"waybill_no": waybill_no, "remarks": remark})
            continue

        if log10_status == 'INITIATED' and zappay_status == 'SUCCESS' and consignment_status == 'DEL' and log10_df['deposit_status'].iloc[0] == 'REQUEST_FOR_APPROVAL':
            remark = "Case 5: Updating statuses for DEL and REQUEST_FOR_APPROVAL deposit"
            print(f"Processing waybill_no({index}/{total_waybills}): {waybill_no}: {remark}")
            for index_prd, row in prd_df.iterrows():
                wb_no = row['waybill_no']
                ptp_query = text("""
                    SELECT id, cod_actual, deposit_settlement_id
                    FROM partner_transaction_payable
                    WHERE waybill_no = :wb_no
                """)
                ptp_df = pd.read_sql_query(ptp_query, log10_engine, params={"wb_no": wb_no})
                if ptp_df.empty:
                    remark = f"Case 5: No partner_transaction_payable data for waybill {wb_no}"
                    print(f"Processing waybill_no({index}/{total_waybills}): {waybill_no}: {remark}")
                    results.append({"waybill_no": waybill_no, "remarks": remark})
                    continue
                si_query = text("""
                    SELECT id, expected_amount, actual_amount
                    FROM settlement_info
                    WHERE id = :deposit_settlement_id
                """)
                si_df = pd.read_sql_query(si_query, log10_engine, params={"deposit_settlement_id": int(ptp_df['deposit_settlement_id'].iloc[0])})
                if si_df.empty:
                    remark = f"Case 5: No settlement_info data for deposit_settlement_id {ptp_df['deposit_settlement_id'].iloc[0]}"
                    print(f"Processing waybill_no({index}/{total_waybills}): {waybill_no}: {remark}")
                    results.append({"waybill_no": waybill_no, "remarks": remark})
                    continue
                expected_amount = si_df['expected_amount'].iloc[0] - ptp_df['cod_actual'].iloc[0]
                actual_amount = si_df['actual_amount'].iloc[0] - ptp_df['cod_actual'].iloc[0]
                id_ = int(ptp_df['deposit_settlement_id'].iloc[0])
                update_si = text("""
                    UPDATE settlement_info
                    SET expected_amount = :expected_amount, actual_amount = :actual_amount
                    WHERE id = :id
                """)
                delete_tsm = text("""
                    DELETE FROM transaction_settlement_mapping
                    WHERE payable_transaction_id = :payable_transaction_id
                """)
                update_cpr = text("""
                    UPDATE consignment_payment_reference_status
                    SET `status` = 'SUCCESS'
                    WHERE payment_reference_number = :payment_reference_number
                """)
                update_ctr = text("""
                    INSERT INTO card_transactions_details (
                        consignment_id, amount, amount_original, currency_code, payment_mode, payment_vendor, status, external_ref_number, external_ref_number1
                    )
                    VALUES (
                        :consignment_id, :amount, :amount_original, :currency_code, :payment_mode, :payment_vendor, :status, :external_ref_number, :external_ref_number1
                    )
                """)
                update_ptp = text("""
                    UPDATE partner_transaction_payable
                    SET payment_mode = 'RIDERAPP_UPI',
                        deposit_settlement_id = hand_over_settlement_id,
                        remitted_settlement_id = hand_over_settlement_id,
                        deposit_status = 'APPROVED',
                        remittance_status = 'APPROVED',
                        new_remittance_status = 'APPROVED',
                        pending_deposit_amount = '0.00',
                        pending_remittance_amount = '0.00',
                        deposit_amount_pending_for_request = '0.00',
                        remittance_amount_pending_for_request = '0.00',
                        deposit_amount_for_approval = '0.00',
                        remittance_amount_for_approval = '0.00'
                    WHERE waybill_no = :wb_no
                """)
                update_c = text("""
                    UPDATE consignments_pod
                    SET payment_type = 'RIDERAPP_UPI'
                    WHERE waybill_no = :wb_no AND shipment_status = 'DEL'
                """)
                update_si2 = text("""
                    UPDATE settlement_info
                    SET payment_option = 'RIDERAPP_UPI'
                    WHERE id IN (
                        SELECT hand_over_settlement_id
                        FROM partner_transaction_payable
                        WHERE waybill_no = :wb_no
                    )
                """)
                update_ct = text("""
                    UPDATE consignment_tracking
                    SET sync_status = 'PENDING',
                        data = JSON_SET(data, '$.paymentType', 'RIDERAPP_UPI')
                    WHERE waybill_number = :wb_no AND event_type = 'DELIVERED'
                """)
                with log10_engine.begin() as conn:
                    conn.execute(update_si, {"expected_amount": expected_amount, "actual_amount": actual_amount, "id": id_})
                    conn.execute(delete_tsm, {"payable_transaction_id": int(ptp_df['id'].iloc[0])})
                    conn.execute(update_cpr, {"payment_reference_number": payment_reference_number})
                    conn.execute(update_ctr, {
                        "consignment_id": int(log10_df['consignment_id'].iloc[0]),
                        "amount": ptp_df['cod_actual'].iloc[0],
                        "amount_original": ptp_df['cod_actual'].iloc[0],
                        "currency_code": 'INR',
                        "payment_mode": 'RIDERAPP_UPI',
                        "payment_vendor": 'RAZORPAY',
                        "status": 'SUCCESS',
                        "external_ref_number": payment_reference_number,
                        "external_ref_number1": payment_reference_number
                    })
                    conn.execute(update_ptp, {"wb_no": wb_no})
                    conn.execute(update_c, {"wb_no": wb_no})
                    conn.execute(update_si2, {"wb_no": wb_no})
                    conn.execute(update_ct, {"wb_no": wb_no})
            results.append({"waybill_no": waybill_no, "remarks": remark})
            continue

        remark = "No issues on this waybill"
        print(f"Processing waybill_no({index}/{total_waybills}): {waybill_no}: {remark}")
        results.append({"waybill_no": waybill_no, "remarks": remark})
    except Exception as e:
        error_msg = f"Error: {str(e)}"
        print(f"Processing waybill_no({index}/{total_waybills}): {waybill_no}: {error_msg}")
        results.append({"waybill_no": waybill_no, "remarks": error_msg})

# Write results to output CSV
try:
    output_df = pd.DataFrame(results)
    output_df.to_csv("output_results.csv", index=False)
    print("Output written to output_results.csv")
except Exception as e:
    print(f"Error writing to CSV: {str(e)}")
