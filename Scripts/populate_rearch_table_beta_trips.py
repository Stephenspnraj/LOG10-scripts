# log10-scripts
from utils import DBManager, Config

inbound_trip_fetch_query = """
    SELECT
	c.id
FROM
	trip_route_location_status trls
JOIN connections c ON
	c.trip_id = trls.trip_id
JOIN locations l ON
	l.id = c.originated_loc_id
	AND l.status = 1
JOIN partners p ON
	p.id = l.entity_id
	AND p.is_virtual = 1
WHERE
	trls.route_location_id = %s
	AND trls.route_location_index <> 0
	AND c.destination_loc_id = %s
    AND (c.created_at BETWEEN (NOW() - INTERVAL 25 HOUR) AND (NOW() - INTERVAL 1 HOUR));
"""
# between now-2hour and now-1hour

entity_consignment_mapping_fetch = """
    SELECT
        ecm.source_entity_id as 'manifestId'
    FROM
        entity_consignment_mapping ecm
    WHERE
        ecm.destination_entity_id = %s
        AND ecm.destination_entity_type = 'CONNECTION';
"""

connection_manifest_mapping_fetch = """
    SELECT
	    cmm.manifest_id as 'manifestId'
    FROM
	    connection_manifest_mapping cmm
    WHERE
	    cmm.connection_id = %s
        AND cmm.is_active = 1;
"""

cmm_insert_query = """
INSERT
	INTO
	connection_manifest_mapping (manifest_id,
	connection_id,
	is_active,
	user_id)
VALUES (%s, %s, %s, %s);
"""

consignment_group_fetch = """
SELECT
	cg.entity_id as 'manifestId',
    cg.entity_code as 'manifestCode',
    cg.consignment_id as 'consignmentId',
    cg.waybill_no as 'waybillNo',
    1 as 'isActive',
    16 as 'userId'
FROM
	consignment_groups cg
WHERE
	cg.entity_id = %s
	AND cg.entity_type = 'MANIFEST';
"""

manifest_consignment_mapping_fetch = """
SELECT
	mcm.manifest_id as 'manifestId',
    mcm.manifest_code as 'manifestCode',
    mcm.consignment_id as 'consignmentId',
    mcm.waybill_no as 'waybillNo',
    1 as 'isActive',
    16 as 'userId'
FROM
	manifest_consignment_mapping mcm
WHERE
	mcm.manifest_id = %s
    AND mcm.is_active = 1;
"""

mcm_insert_query = """
INSERT
	INTO
	    manifest_consignment_mapping (manifest_id,
	    manifest_code,
	    consignment_id,
	    waybill_no,
        is_active,
	    user_id)
VALUES (%s, %s, %s, %s, %s, %s);
"""


def populate_rearch_tables():

    try:

        configObj = Config()
        db = DBManager(configObj.hydra_prime_credentials)

        locations_to_check = [5882708,5882931,5879892,5883512,5881109,5881032]  # locations DHS,GAS,GHS,HGS,LUS,DSS

        for location in locations_to_check:

            connections = db.fetchDataTuple(inbound_trip_fetch_query, (location, location)).fetchall()

            print(f"connections: {connections}")

            for connection_record in connections:
                connection_id = connection_record[0]
                print(f"Processing connection_id: {connection_id}")

                ecmData = set(db.fetchDataTuple(entity_consignment_mapping_fetch, (connection_id,)).fetchall())
                cmmData = set(db.fetchDataTuple(connection_manifest_mapping_fetch, (connection_id,)).fetchall())

                difference_manifest_ids = ecmData - cmmData
                manifest_data_insert = []
                consignment_data_insert = []

                print(f"difference_manifest_ids: {difference_manifest_ids}")

                for manifest in difference_manifest_ids:

                    manifest_id = manifest[0]

                    manifest_data_insert.append((manifest_id, connection_id, 1, 16))

                    cgData = set(db.fetchDataTuple(consignment_group_fetch, (manifest_id,)).fetchall())
                    mcmData = set(db.fetchDataTuple(manifest_consignment_mapping_fetch, (manifest_id,)).fetchall())

                    difference_consignment_data = cgData - mcmData
                    consignment_data_insert.extend(difference_consignment_data)

                print(f"Manifest IDs to insert: {manifest_data_insert}")
                print(f"Consignments to insert: {consignment_data_insert}")

                if manifest_data_insert:
                    db.executemany(cmm_insert_query, manifest_data_insert)

                if consignment_data_insert:
                    db.executemany(mcm_insert_query, consignment_data_insert)

                db.commit()


        print("Transaction committed successfully.")

    except Exception as err:
        print(f"Error: {err}")
        db.rollback()
        print("Transaction rolled back due to an error.")
    finally:
        if db:
            db.close()
            print("MySQL connection closed")

populate_rearch_tables()
