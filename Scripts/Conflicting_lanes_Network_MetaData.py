import pandas as pd
import pymysql
from collections import defaultdict
import os

# DB Connection
conn = pymysql.connect(
    host="log10-replica-replica.cco3osxqlq4g.ap-south-1.rds.amazonaws.com",
    user="log10_scripts",
    password="D2lx7Wz0Wm9ISAY-Vp1gxKDTzLRRl5k1m",
    database="loadshare",
    port=3306
)
cursor = conn.cursor()

# ------------------------------------------------------
# Fetch data
# ------------------------------------------------------
query = """
SELECT id, location_alias, crossdock_alias, next_location_alias
FROM network_metadata
WHERE is_active = 1
"""
cursor.execute(query)
rows = cursor.fetchall()

# ------------------------------------------------------
# Clean values
# ------------------------------------------------------
def clean(val):
    if isinstance(val, str):
        val = (
            val.replace(".LMSC", "")
               .replace(".FMSC", "")
               .strip()
        )
        return val if val else None
    return val

df = pd.DataFrame(
    [tuple(clean(c) for c in row) for row in rows],
    columns=["id", "location", "crossdock", "next_loc"]
)

# ------------------------------------------------------
# Normalize NULLs (for Rule 1 duplicate detection)
# ------------------------------------------------------
df["_crossdock_norm"] = df["crossdock"].fillna("<NULL>")

# ------------------------------------------------------
# Helper
# ------------------------------------------------------
def fmt_path(a, b, c):
    return f"{a} → {b} → {c}"

violations = []

# ------------------------------------------------------
# Rule 1: Exact duplicate (A,B,C) including NULLs
# ------------------------------------------------------
dup_df = df[df.duplicated(
    ["location", "_crossdock_norm", "next_loc"],
    keep=False
)]

for _, g in dup_df.groupby(["location", "_crossdock_norm", "next_loc"]):
    for _, r1 in g.iterrows():
        for _, r2 in g.iterrows():
            if r1["id"] != r2["id"]:
                violations.append({
                    "id": r1["id"],
                    "location": r1["location"],
                    "crossdock": r1["crossdock"],
                    "next_loc": r1["next_loc"],
                    "match_id": r2["id"],
                    "match_path": fmt_path(
                        r2["location"], r2["crossdock"], r2["next_loc"]
                    ),
                    "rule": "Rule1 (Exact duplicate A,B,C)"
                })

# ------------------------------------------------------
# Prepare renamed copies (used by Rules 2–6)
# ------------------------------------------------------
left = df.rename(columns={
    "id": "id_src",
    "location": "A",
    "crossdock": "B",
    "next_loc": "C"
})

right = df.rename(columns={
    "id": "id_match",
    "location": "A2",
    "crossdock": "B2",
    "next_loc": "C2"
})

# ------------------------------------------------------
# Rule 2: Reverse path (A,B,C ↔ C,B,A)
# ------------------------------------------------------
rule2 = left.merge(
    right,
    left_on=["A", "B", "C"],
    right_on=["C2", "B2", "A2"]
)

for _, r in rule2.iterrows():
    if r["id_src"] != r["id_match"]:
        violations.append({
            "id": r["id_src"],
            "location": r["A"],
            "crossdock": r["B"],
            "next_loc": r["C"],
            "match_id": r["id_match"],
            "match_path": fmt_path(r["A2"], r["B2"], r["C2"]),
            "rule": "Rule2 (Reverse path A,B,C ↔ C,B,A)"
        })

# ------------------------------------------------------
# Rule 3: Same A & C, different B
# ------------------------------------------------------
rule3 = left.merge(
    right,
    left_on=["A", "C"],
    right_on=["A2", "C2"]
)

for _, r in rule3.iterrows():
    if r["id_src"] != r["id_match"] and r["B"] != r["B2"]:
        violations.append({
            "id": r["id_src"],
            "location": r["A"],
            "crossdock": r["B"],
            "next_loc": r["C"],
            "match_id": r["id_match"],
            "match_path": fmt_path(r["A2"], r["B2"], r["C2"]),
            "rule": "Rule3 (Same A & C, different B)"
        })

# ------------------------------------------------------
# Rule 4: (A,B,C → B,X,A)
# ------------------------------------------------------
rule4 = left.merge(
    right,
    left_on=["B", "A"],
    right_on=["A2", "C2"]
)

for _, r in rule4.iterrows():
    if r["id_src"] != r["id_match"]:
        violations.append({
            "id": r["id_src"],
            "location": r["A"],
            "crossdock": r["B"],
            "next_loc": r["C"],
            "match_id": r["id_match"],
            "match_path": fmt_path(r["A2"], r["B2"], r["C2"]),
            "rule": "Rule4 (A,B,C → B,X,A)"
        })

# ------------------------------------------------------
# Rule 5: (A,B,C → A,X,B) where X is NOT null/empty
# ------------------------------------------------------
rule5 = left.merge(
    right,
    left_on=["A", "B"],
    right_on=["A2", "C2"]
)

for _, r in rule5.iterrows():
    x = r["B2"]  # X

    if (
        r["id_src"] != r["id_match"]
        and x is not None
        and str(x).strip() != ""
    ):
        violations.append({
            "id": r["id_src"],
            "location": r["A"],
            "crossdock": r["B"],
            "next_loc": r["C"],
            "match_id": r["id_match"],
            "match_path": fmt_path(r["A2"], r["B2"], r["C2"]),
            "rule": "Rule5 (A,B,C → A,X,B, X present)"
        })

# ------------------------------------------------------
# Rule 6: (A,B,C → C,D,B)
# ------------------------------------------------------
rule6 = left.merge(
    right,
    left_on=["C", "B"],
    right_on=["A2", "C2"]
)

for _, r in rule6.iterrows():
    if r["id_src"] != r["id_match"]:
        violations.append({
            "id": r["id_src"],
            "location": r["A"],
            "crossdock": r["B"],
            "next_loc": r["C"],
            "match_id": r["id_match"],
            "match_path": fmt_path(r["A2"], r["B2"], r["C2"]),
            "rule": "Rule6 (A,B,C → C,D,B)"
        })

# ------------------------------------------------------
# Final Output
# ------------------------------------------------------
final_df = (
    pd.DataFrame(violations)
    .drop_duplicates()
    .sort_values(["id", "rule", "match_id"])
)

final_df = final_df[
    ["id", "location", "crossdock", "next_loc",
     "match_id", "match_path", "rule"]
]

# ------------------------------------------------------
# Print
# ------------------------------------------------------
print("\n✅ Clear Rule Violations")
print(final_df.to_string(index=False))

# --- Excel Export ---
# --- Excel Export ---
excel_path = "network_rules.xlsx"

with pd.ExcelWriter(excel_path, engine="xlsxwriter") as writer:
    final_df.to_excel(writer, sheet_name="Violations", index=False)

print(f"\n✅ Excel exported successfully: {excel_path}")
