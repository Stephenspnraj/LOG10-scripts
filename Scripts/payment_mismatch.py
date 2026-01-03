from datetime import datetime
import os
import pandas as pd
import pytz
import requests
from sqlalchemy import create_engine, text
import sys

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

log10_url = f"mysql+mysqlconnector://{log10_db['user']}:{log10_db['password']}@{log10_db['host']}:{log10_db['port']}/{log10_db['dbname']}"

titan_url = f"mysql+mysqlconnector://{titan_db['user']}:{titan_db['password']}@{titan_db['host']}:{titan_db['port']}/{titan_db['dbname']}"

log10_engine = create_engine(log10_url)

titan_engine = create_engine(titan_url)

waybill_no = os.environ['waybill_no']

log10_query = f"""
select
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
	c.updated_at,
	ptp.deposit_status
from
	consignment_payment_reference_status ps
join consignment_payment_reference_details prd on
	ps.payment_reference_number = prd.payment_reference_number 
join consignments c on prd.consignment_id = c.id 
left join partner_transaction_payable ptp on ptp.waybill_no = c.waybill_no 
where
	prd.waybill_no = '{waybill_no}'
"""

log10_df = pd.read_sql_query(log10_query, log10_engine)

print(log10_df)

log10_status = log10_df.iloc[0][6]
print("log10_status :",log10_status)
payment_reference_number = log10_df.iloc[0][3]
print("payment_reference_number :",payment_reference_number)

consignment_status = log10_df.iloc[0][10]
print("consignment_status :",consignment_status)

titan_query = f"""
select
	amount as "order_Amount",
	source_link_id as referenceId,
	transaction_status as txStatus,
	bank_reference_number as utrNumber ,
	entity_value as trackingId,
	entity_id as runsheetId,
	link_status,
	is_deleted,
	payment_at,
	plc.updated_at,
	plc.created_at,
	transaction_id,
	pv.name 
from
	zappay.payment_links_cod plc
join zappay.payment_vendors pv on plc.payment_vendor_id = pv.id
where
	source_link_id = '{payment_reference_number}';
"""

titan_df = pd.read_sql_query(titan_query, titan_engine)

print("titandf",titan_df)

prd_wbs = f"""
SELECT
	distinct waybill_no as waybill_no
from
	consignment_payment_reference_details
where
	payment_reference_number = '{payment_reference_number}';
"""

prd_df = pd.read_sql_query(prd_wbs, log10_engine)

zappay_status = titan_df.iloc[0][2]

if zappay_status != "SUCCESS":
    print("Case 1")
    print("Please contact Meesho")
    sys.exit()

ofd_at = pd.to_datetime(log10_df.iloc[0][11])

if ofd_at.tzinfo is None:
    india_tz = pytz.timezone('Asia/Kolkata')
    ofd_at = india_tz.localize(ofd_at)

india_tz = pytz.timezone('Asia/Kolkata')
today = datetime.now(india_tz)

today_midnight = india_tz.localize(datetime(today.year, today.month, today.day))

if log10_status == 'INITIATED' and zappay_status == 'SUCCESS' and consignment_status == 'OFD' and ofd_at > today_midnight:
    print("Case 2")
    url = "https://meesho-api.loadshare.net/lm/external/payment/webhook"

    headers = {
        "token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ2ZW5kb3IiOiJIZXJtZXMiLCJlbnYiOiJQcm9kdWN0aW9uIn0.DL6OMhM5DFz9-Mp3b7Iz1cigKpD4iEwi3yuCTsUGmlA",
        "Content-Type": "application/json"
    }

    data = {
        "orderAmount": int(log10_df.iloc[0][5]),
        "paymentMode": "RIDERAPP_UPI",
        "referenceId": log10_df.iloc[0][3],
        "txStatus": "SUCCESS",
        "runsheetId": int(titan_df.iloc[0][5]),
        "trackingId": [
            titan_df.iloc[0][4]
        ],
        "taskId": [
            int(log10_df.iloc[0][0])
        ],
        "txMsg": "",
        "txTime": "",
        "utrNumber": ""
    }

    print("Payload data:", data)

    response = requests.post(url, headers=headers, json=data)

    if response.status_code == 200:
        print("Request was successful")
    else:
        print(f"Request failed with status code {response.status_code}")
    sys.exit()

if log10_status == 'INITIATED' and zappay_status == 'SUCCESS' and consignment_status == 'OFD' and ofd_at < today_midnight:
    print("Case 3")
    update_query = f"""
    UPDATE
	    consignment_payment_reference_status
    SET
	    `status` = 'SUCCESS'
    WHERE
	    payment_reference_number = '{payment_reference_number}';

    """

    print(update_query)

    with log10_engine.connect() as conn:
        conn.execute(text(update_query))

    insert_query = f"""
    INSERT INTO card_transactions_details 
        (`consignment_id`,
        `amount`,
        `amount_original`,
        `currency_code`,
        `payment_mode`,
        `payment_vendor`,
        `status`,
        `external_ref_number`,
        `external_ref_number1`)
    VALUES 
        ({int(log10_df.iloc[0][0])},
        {int(log10_df.iloc[0][5])},
        {int(log10_df.iloc[0][5])},
        'INR',
        'RIDERAPP_UPI',
        'RAZORPAY',
        'SUCCESS',
        '{payment_reference_number}', 
        '{payment_reference_number}');
    """

    print(insert_query)

    with log10_engine.connect() as conn:
        conn.execute(text(insert_query))
        
    sys.exit()

if log10_status == 'INITIATED' and zappay_status == 'SUCCESS' and consignment_status == 'DEL' and log10_df.iloc[0][12] == 'PENDING':
    print("Case 4")
    for index, row in prd_df.iterrows():

        waybill_no = row['waybill_no']

        update_query = f"""
        UPDATE consignment_payment_reference_status 
        SET `status` = 'SUCCESS' WHERE 
        payment_reference_number='{payment_reference_number}';
        """

        print(update_query)

        with log10_engine.connect() as conn:
            conn.execute(text(update_query))

        insert_query = f"""
        INSERT INTO card_transactions_details 
            (consignment_id,
            amount,
            amount_original,
            currency_code,
            payment_mode,
            payment_vendor,
            status,
            external_ref_number,
            external_ref_number1)
        VALUES 
            ('{log10_df.iloc[0][0]}',
            '{log10_df.iloc[0][5]}',
            '{log10_df.iloc[0][5]}',
            'INR',
            '{log10_df.iloc[0][4]}', 
            '{titan_df.iloc[0][12]}',
            '{zappay_status}',
            '{payment_reference_number}', 
            '{payment_reference_number}');
        """

        print(insert_query)

        with log10_engine.connect() as conn:
            conn.execute(text(insert_query))

        update_ptp = f"""
        UPDATE
        partner_transaction_payable
        SET
        payment_mode = 'RIDERAPP_UPI',
        deposit_settlement_id = hand_over_settlement_id,
        remitted_settlement_id = 
        hand_over_settlement_id,
        deposit_status = 'APPROVED',
        remittance_status = 'APPROVED',
        new_remittance_status = 'APPROVED',
        pending_deposit_amount = '0.00',
        pending_remittance_amount = '0.00',
        deposit_amount_pending_for_request = '0.00',
        remittance_amount_pending_for_request = '0.00',
        deposit_amount_for_approval = '0.00',
        remittance_amount_for_approval = '0.00'
        WHERE
        waybill_no = '{waybill_no}';
        """

        print(update_ptp)

        with log10_engine.connect() as conn:
            conn.execute(text(update_ptp))

        cpod_update = f"""
        UPDATE
        consignments_pod
        SET
        payment_type = 'RIDERAPP_UPI'
        WHERE
        waybill_no = '{waybill_no}'
        and shipment_status = 'DEL';
        """

        print(cpod_update)

        with log10_engine.connect() as conn:
            conn.execute(text(cpod_update))

        si_update = f"""
        UPDATE
            settlement_info
        SET
            payment_option = 'RIDERAPP_UPI'
        WHERE
            id in (
            select
                hand_over_settlement_id
            from
                partner_transaction_payable
            where
                waybill_no = '{waybill_no}')
        """

        print(si_update)

        with log10_engine.connect() as conn:
            conn.execute(text(si_update))

        ct_query = f"""
        UPDATE
        consignment_tracking
        SET
        sync_status = 'PENDING',
        data = JSON_SET(data, 
        '$.paymentType', 'RIDERAPP_UPI')
        WHERE
        waybill_number = '{waybill_no}'
        and event_type = 'DELIVERED';
        """

        print(ct_query)

        with log10_engine.connect() as conn:
            conn.execute(text(ct_query))

    sys.exit()

if log10_status == 'INITIATED' and zappay_status == 'SUCCESS' and consignment_status == 'DEL' and log10_df.iloc[0][12] == 'REQUEST_FOR_APPROVAL':
    print("Case 5")
    for index, row in prd_df.iterrows():
        waybill_no = row['waybill_no']
        ptp_id = f"""
        select
            id,
            cod_actual,
            deposit_settlement_id
        from
            partner_transaction_payable
        where
            waybill_no = '{waybill_no}';
        """

        ptp_df = pd.read_sql_query(ptp_id, log10_engine)

        si_id = f"""
        select
            id,
            expected_amount,
            actual_amount
        from
            settlement_info
        where
            id = {int(ptp_df.iloc[0][2])}
        """

        si_df = pd.read_sql_query(si_id, log10_engine)

        expected_amount = si_df.iloc[0][1]-ptp_df.iloc[0][1]

        actual_amount = si_df.iloc[0][2]-ptp_df.iloc[0][1]

        id = int(ptp_df.iloc[0][2])

        update_si = f"""
        update
            settlement_info
        set
            expected_amount = {expected_amount},
            actual_amount = {actual_amount}
        where
            id = {id};
        """
        with log10_engine.connect() as conn:
            conn.execute(text(update_si))
            print(update_si)

        delete_tsm = f"""
        delete
        from
            transaction_settlement_mapping
        where
            payable_transaction_id = {int(ptp_df.iloc[0][0])};
        """
        with log10_engine.connect() as conn:
            conn.execute(text(delete_tsm))
            print(delete_tsm)

        update_cpr = f"""
        UPDATE
            consignment_payment_reference_status
        SET
            `status` = 'SUCCESS'
        WHERE
            payment_reference_number = '{payment_reference_number}';
        """
        with log10_engine.connect() as conn:
            conn.execute(text(update_cpr))
            print(update_cpr)

        update_ctr = f"""
            INSERT INTO card_transactions_details (
            consignment_id,
            amount,
            amount_original,
            currency_code,
            payment_mode,
            payment_vendor,
            status,
            external_ref_number,
            external_ref_number1
        )
        VALUES
        (
            '{int(log10_df.iloc[0][0])}',
            '{ptp_df.iloc[0][1]}',
            '{ptp_df.iloc[0][1]},',
            'INR',
            'RIDERAPP_UPI',
            'RAZORPAY',
            'SUCCESS',
            '{payment_reference_number}',
            '{payment_reference_number}'
        );
        """
        with log10_engine.connect() as conn:
            conn.execute(text(update_ctr))
            print(update_ctr)

        update_ptp = f"""
        UPDATE
            loadshare.partner_transaction_payable
        SET
            payment_mode = 'RIDERAPP_UPI',
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
        WHERE
            waybill_no = '{waybill_no}';
        """
        with log10_engine.connect() as conn:
            conn.execute(text(update_ptp))
            print(update_ptp)

        update_c = f"""
        UPDATE
            consignments_pod
        SET
            payment_type = 'RIDERAPP_UPI'
        WHERE
            waybill_no = '{waybill_no}'
            and shipment_status = 'DEL';
        """
        with log10_engine.connect() as conn:
            conn.execute(text(update_c))
            print(update_c)

        update_si = f"""
        UPDATE
            settlement_info
        SET
            payment_option = 'RIDERAPP_UPI'
        WHERE
            id in (
                select
                    hand_over_settlement_id
                from
                    partner_transaction_payable
                where
                    waybill_no = '{waybill_no}'
            );
        """
        with log10_engine.connect() as conn:
            conn.execute(text(update_si))
            print(update_si)

        update_ct = f"""
        UPDATE
            consignment_tracking
        SET
            sync_status = 'PENDING',
            data = JSON_SET(data, '$.paymentType', 'RIDERAPP_UPI')
        WHERE
            waybill_number = '{waybill_no}'
            and event_type = 'DELIVERED';
        """
        with log10_engine.connect() as conn:
            conn.execute(text(update_ct))
            print(update_ct)
    sys.exit()

print("No issues on this waybill")
