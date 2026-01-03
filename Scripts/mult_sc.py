import os
import pandas as pd
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from tabulate import tabulate
import time


# Database credentials
LOG10_DB_CREDS = {
    "user": "log10_scripts",
    "password": "D2lx7Wz0Wm9ISAY-Vp1gxKDTzLRRl5k1m",
    "host": "log10-replica-replica.cco3osxqlq4g.ap-south-1.rds.amazonaws.com",
    "port": "3306",
    "dbname": "loadshare"
}


# Create database engine
log10_engine = create_engine(
    f"mysql+mysqlconnector://{LOG10_DB_CREDS['user']}:{quote_plus(LOG10_DB_CREDS['password'])}"
    f"@{LOG10_DB_CREDS['host']}:{LOG10_DB_CREDS['port']}/{LOG10_DB_CREDS['dbname']}"
)


try:
    truncate_flag = int(os.environ.get('Delete_Add', 0))
except ValueError:
    print("Error: Delete_Add must be an integer")
    exit(1)


# Step 1: Truncate existing mappings related to multi-sc wrong facility if requested
if truncate_flag == 1:
    with log10_engine.connect() as conn:
        result = conn.execute(text("""
            SELECT id FROM next_location_configs
            WHERE audit_log LIKE 'multi-sc wrong facil%'
        """))
        ids = [row[0] for row in result]
    print(f"Found {len(ids)} rows to delete")
    batch_size = 1000
    for i in range(0, len(ids), batch_size):
        batch_ids = ids[i:i + batch_size]
        with log10_engine.begin() as conn:
            conn.execute(text("""
                DELETE FROM next_location_configs
                WHERE id IN :ids
            """), {"ids": tuple(batch_ids)})
        print(f"Deleted batch {i // batch_size + 1} of {len(batch_ids)} rows")
        time.sleep(1)
    print("All deletions complete.")


# Step 2: Fetch all active reroute pairs from path_correction_config
with log10_engine.connect() as conn:
    reroute_df = pd.read_sql_query("""
        SELECT id, old_loc, new_loc
        FROM path_correction_config
        WHERE loc_type = 'REROUTE' AND status = 1
        ORDER BY id
    """, conn)


if reroute_df.empty:
    print("No active reroute pairs found")
    exit(0)


customer_id = 10823  # Use the actual customer_id as required


def fetch_aliases(loc_ids):
    if not loc_ids:
        return {}
    with log10_engine.connect() as conn:
        query = f"""
            SELECT id, alias
            FROM locations
            WHERE id IN ({','.join(map(str, loc_ids))})
              AND entity_type = 'PARTNER' AND status = 1
        """
        alias_df = pd.read_sql_query(query, conn)
    return dict(zip(alias_df['id'], alias_df['alias']))


def fetch_pincode_aliases(pincode_ids):
    if not pincode_ids:
        return {}
    with log10_engine.connect() as conn:
        query = f"""
            SELECT pincode_id, alias
            FROM locations
            WHERE pincode_id IN ({','.join(map(str, pincode_ids))})
              AND entity_type = 'PARTNER' AND status = 1
            GROUP BY pincode_id
        """
        df = pd.read_sql_query(query, conn)
    return dict(zip(df['pincode_id'], df['alias']))


def fetch_lmdcs(old_alias, new_alias):
    query = f"""
        SELECT
            l.id AS lm_location_id,
            l.alias AS lm_alias,
            cl.id AS sc_location_id,
            cl.alias AS sc_alias,
            l.pincode_id AS lm_location_pincode_id
        FROM
            network_metadata nm
        JOIN locations l ON nm.next_location_alias = l.alias
            AND l.entity_type = 'PARTNER'
            AND l.status = 1
        JOIN locations cl ON cl.alias IN ('{old_alias}', '{new_alias}')
            AND cl.entity_type = 'PARTNER'
            AND cl.status = 1
        WHERE
            nm.location_alias = CONCAT(cl.alias, '.LMSC')
            AND nm.is_active = 1
            AND l.location_ops_type = 'LM'
            AND (nm.crossdock_alias IS NULL OR nm.crossdock_alias = '')
            AND l.entity_id NOT IN ('127788', '127798', '127869', '128146')
    """
    with log10_engine.connect() as conn:
        lm_df = pd.read_sql_query(query, conn)
    return lm_df


def fetch_existing_mappings():
    with log10_engine.connect() as conn:
        existing_df = pd.read_sql_query("""
            SELECT customer_id, location_id, next_location_id, pincode_id, entity_type, is_active
            FROM next_location_configs
            WHERE entity_type='MANIFEST' AND is_active=1
        """, conn)
    existing_df['key'] = existing_df.apply(
        lambda x: (x['customer_id'], x['location_id'], x['next_location_id'], x['pincode_id'], x['entity_type'], x['is_active']), axis=1
    )
    return set(existing_df['key'])


def fetch_location_pincode(loc_id):
    loc_id_int = int(loc_id)
    with log10_engine.connect() as conn:
        result = conn.execute(text("""
            SELECT pincode_id FROM locations WHERE id = :loc_id
        """), {'loc_id': loc_id_int}).fetchone()
    return result[0] if result else None


existing_mappings_keys = fetch_existing_mappings()

inserted_records = []
inserted_count_summary = {}

for idx, row in reroute_df.iterrows():
    old_sc_id = row['old_loc']
    new_sc_id = row['new_loc']

    aliases = fetch_aliases([old_sc_id, new_sc_id])
    old_sc_alias = aliases.get(old_sc_id, '')
    new_sc_alias = aliases.get(new_sc_id, '')

    if not old_sc_alias or not new_sc_alias:
        print(f"Skipping id={row['id']}: Alias missing (old: {old_sc_alias}, new: {new_sc_alias})")
        continue

    if old_sc_alias == new_sc_alias:
        print(f"Skipping id={row['id']}: old and new aliases are same '{old_sc_alias}'")
        continue

    print(f"\nProcessing reroute id={row['id']}: {old_sc_alias} -> {new_sc_alias}")

    lm_df = fetch_lmdcs(old_sc_alias, new_sc_alias)

    old_sc_lm = lm_df[lm_df['sc_alias'] == old_sc_alias]
    new_sc_lm = lm_df[lm_df['sc_alias'] == new_sc_alias]

    if old_sc_lm.empty or new_sc_lm.empty:
        print(f"Skipping id={row['id']}: No LMDC found for one or both SCs (old LMDC count: {len(old_sc_lm)}, new LMDC count: {len(new_sc_lm)})")
        continue

    old_sc_pincode = fetch_location_pincode(old_sc_id)
    new_sc_pincode = fetch_location_pincode(new_sc_id)

    mappings = []

    # Forward mappings (old to new)
    for _, old_lm in old_sc_lm.iterrows():
        for _, new_lm in new_sc_lm.iterrows():
            print(f"  Forward mapping LMDC combo: {old_lm['lm_alias']}({old_lm['lm_location_id']}) -> {new_lm['lm_alias']}({new_lm['lm_location_id']})")

            entry = (customer_id, old_lm['lm_location_id'], old_sc_id, new_lm['lm_location_pincode_id'], 'MANIFEST', 1)
            if entry not in existing_mappings_keys:
                mappings.append({
                    'customer_id': customer_id,
                    'location_id': old_lm['lm_location_id'],
                    'next_location_id': old_sc_id,
                    'pincode_id': new_lm['lm_location_pincode_id'],
                    'return_available': 1,
                    'entity_type': 'MANIFEST',
                    'is_active': 1,
                    'audit_log': 'multi-sc wrong facility',
                    'is_manual': 1
                })
                existing_mappings_keys.add(entry)

            entry = (customer_id, old_sc_id, new_sc_id, new_lm['lm_location_pincode_id'], 'MANIFEST', 1)
            if entry not in existing_mappings_keys:
                mappings.append({
                    'customer_id': customer_id,
                    'location_id': old_sc_id,
                    'next_location_id': new_sc_id,
                    'pincode_id': new_lm['lm_location_pincode_id'],
                    'return_available': 1,
                    'entity_type': 'MANIFEST',
                    'is_active': 1,
                    'audit_log': 'multi-sc wrong facility',
                    'is_manual': 1
                })
                existing_mappings_keys.add(entry)

            entry = (customer_id, new_sc_id, new_lm['lm_location_id'], new_lm['lm_location_pincode_id'], 'MANIFEST', 1)
            if entry not in existing_mappings_keys:
                mappings.append({
                    'customer_id': customer_id,
                    'location_id': new_sc_id,
                    'next_location_id': new_lm['lm_location_id'],
                    'pincode_id': new_lm['lm_location_pincode_id'],
                    'return_available': 1,
                    'entity_type': 'MANIFEST',
                    'is_active': 1,
                    'audit_log': 'multi-sc wrong facility',
                    'is_manual': 1
                })
                existing_mappings_keys.add(entry)

        entry = (customer_id, old_lm['lm_location_id'], old_sc_id, new_sc_pincode, 'MANIFEST', 1)
        if entry not in existing_mappings_keys:
            mappings.append({
                'customer_id': customer_id,
                'location_id': old_lm['lm_location_id'],
                'next_location_id': old_sc_id,
                'pincode_id': new_sc_pincode,
                'return_available': 1,
                'entity_type': 'MANIFEST',
                'is_active': 1,
                'audit_log': 'multi-sc wrong facility rto',
                'is_manual': 1
            })
            existing_mappings_keys.add(entry)

    # Reverse mappings (new to old)
    for _, new_lm in new_sc_lm.iterrows():
        for _, old_lm in old_sc_lm.iterrows():
            print(f"  Reverse mapping LMDC combo: {new_lm['lm_alias']}({new_lm['lm_location_id']}) -> {old_lm['lm_alias']}({old_lm['lm_location_id']})")

            entry = (customer_id, new_lm['lm_location_id'], new_sc_id, old_lm['lm_location_pincode_id'], 'MANIFEST', 1)
            if entry not in existing_mappings_keys:
                mappings.append({
                    'customer_id': customer_id,
                    'location_id': new_lm['lm_location_id'],
                    'next_location_id': new_sc_id,
                    'pincode_id': old_lm['lm_location_pincode_id'],
                    'return_available': 1,
                    'entity_type': 'MANIFEST',
                    'is_active': 1,
                    'audit_log': 'multi-sc wrong facility',
                    'is_manual': 1
                })
                existing_mappings_keys.add(entry)

            entry = (customer_id, new_sc_id, old_sc_id, old_lm['lm_location_pincode_id'], 'MANIFEST', 1)
            if entry not in existing_mappings_keys:
                mappings.append({
                    'customer_id': customer_id,
                    'location_id': new_sc_id,
                    'next_location_id': old_sc_id,
                    'pincode_id': old_lm['lm_location_pincode_id'],
                    'return_available': 1,
                    'entity_type': 'MANIFEST',
                    'is_active': 1,
                    'audit_log': 'multi-sc wrong facility',
                    'is_manual': 1
                })
                existing_mappings_keys.add(entry)

            entry = (customer_id, old_sc_id, old_lm['lm_location_id'], old_lm['lm_location_pincode_id'], 'MANIFEST', 1)
            if entry not in existing_mappings_keys:
                mappings.append({
                    'customer_id': customer_id,
                    'location_id': old_sc_id,
                    'next_location_id': old_lm['lm_location_id'],
                    'pincode_id': old_lm['lm_location_pincode_id'],
                    'return_available': 1,
                    'entity_type': 'MANIFEST',
                    'is_active': 1,
                    'audit_log': 'multi-sc wrong facility',
                    'is_manual': 1
                })
                existing_mappings_keys.add(entry)

        entry = (customer_id, new_lm['lm_location_id'], new_sc_id, old_sc_pincode, 'MANIFEST', 1)
        if entry not in existing_mappings_keys:
            mappings.append({
                'customer_id': customer_id,
                'location_id': new_lm['lm_location_id'],
                'next_location_id': new_sc_id,
                'pincode_id': old_sc_pincode,
                'return_available': 1,
                'entity_type': 'MANIFEST',
                'is_active': 1,
                'audit_log': 'multi-sc wrong facility rto',
                'is_manual': 1
            })
            existing_mappings_keys.add(entry)

    if not mappings:
        print(f"No new mappings to insert for reroute id={row['id']} ({old_sc_alias} -> {new_sc_alias})")
        continue

    new_nlc_df = pd.DataFrame(mappings)
    inserted_records.append(new_nlc_df)
    print(f"Inserting {len(new_nlc_df)} new mappings for reroute id={row['id']} ({old_sc_alias} -> {new_sc_alias})")

    try:
        with log10_engine.begin() as conn:
            new_nlc_df.to_sql(
                name="next_location_configs",
                schema="loadshare",
                con=conn,
                if_exists="append",
                index=False,
                chunksize=1000
            )
        print(f"Successfully inserted {len(new_nlc_df)} mappings")
        inserted_count_summary[(old_sc_alias, new_sc_alias)] = inserted_count_summary.get((old_sc_alias, new_sc_alias), 0) + len(new_nlc_df)
    except Exception as e:
        print(f"Insertion failed: {e}. Attempting row-by-row insertion.")
        inserted_count_successful = 0
        with log10_engine.begin() as conn:
            for _, row_ins in new_nlc_df.iterrows():
                try:
                    row_ins.to_frame().T.to_sql(
                        name="next_location_configs",
                        schema="loadshare",
                        con=conn,
                        if_exists="append",
                        index=False
                    )
                    print(f"Inserted row: {row_ins.to_dict()}")
                    inserted_count_successful += 1
                except Exception as e_row:
                    print(f"Failed to insert row {row_ins.to_dict()}: {e_row}")
        inserted_count_summary[(old_sc_alias, new_sc_alias)] = inserted_count_summary.get((old_sc_alias, new_sc_alias), 0) + inserted_count_successful


# After all processed, write inserted mappings log file with aliases for location_id, next_location_id and pincodes

if inserted_records:
    all_inserted_df = pd.concat(inserted_records, ignore_index=True)

    # Fetch aliases mapping for location_id and next_location_id
    all_location_ids = set(all_inserted_df['location_id']).union(set(all_inserted_df['next_location_id']))
    location_alias_map = fetch_aliases(list(all_location_ids))

    # Fetch aliases for pincodes by querying locations with matching pincode_ids
    all_pincode_ids = set(all_inserted_df['pincode_id'])
    pincode_alias_map = {}
    if all_pincode_ids:
        with log10_engine.connect() as conn:
            # Get one alias per pincode (first one if multiple)
            pincode_alias_df = pd.read_sql_query(f"""
                SELECT pincode_id, alias FROM locations 
                WHERE pincode_id IN ({','.join(map(str, all_pincode_ids))})
                AND entity_type = 'PARTNER' AND status = 1
                GROUP BY pincode_id
            """, conn)
        pincode_alias_map = dict(zip(pincode_alias_df['pincode_id'], pincode_alias_df['alias']))

    # Map aliases
    all_inserted_df['location_alias'] = all_inserted_df['location_id'].map(location_alias_map)
    all_inserted_df['next_location_alias'] = all_inserted_df['next_location_id'].map(location_alias_map)
    all_inserted_df['pincode_alias'] = all_inserted_df['pincode_id'].map(pincode_alias_map)

    # Specify output columns with aliases included
    cols = [
        'customer_id',
        'location_id', 'location_alias',
        'next_location_id', 'next_location_alias',
        'pincode_id', 'pincode_alias',
        'return_available',
        'entity_type',
        'is_active',
        'audit_log',
        'is_manual'
    ]

    all_inserted_df.to_csv('inserted_mappings_log.csv', index=False, columns=cols)
    print(f"\nWritten all inserted mappings with aliases to 'inserted_mappings_log.csv'")

# Final summary output
if inserted_count_summary:
    summary_df = pd.DataFrame(
        [{'Old_SC': k[0], 'New_SC': k[1], 'Inserted_Mappings': v} for k, v in inserted_count_summary.items()]
    )
    print("\nSummary of inserted mappings per reroute pair:")
    print(tabulate(summary_df, headers='keys', tablefmt='fancy_grid', showindex=False))
else:
    print("No new mappings were inserted during this run.")


print("Script completed")
