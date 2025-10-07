SUPPORTED_COLUMN_CHECKS = ["UNIQUE","NULL_COUNT","MIN_MAX","WHITESPACE","FORMAT_DISTRIBUTION","VALUE_DISTRIBUTION"]
SUPPORTED_TABLE_CHECKS  = ["FRESHNESS","ROW_COUNT"]

def _q(ident: str) -> str:
    parts = [p.strip('"') for p in ident.split('.')]
    return '.'.join([f'"{p}"' for p in parts])

def build_rule_for_column_check(fqn: str, col: str, ctype: str, params: dict):
    colq = f'"{col}"'
    # return row predicate SQL, is_agg=False
    ctype = (ctype or "").upper()
    if ctype == "UNIQUE":
        ignore_nulls = bool(params.get("ignore_nulls", True))
        if ignore_nulls:
            return f"({colq} IS NULL OR {colq} IN (SELECT {colq} FROM {_q(fqn)} GROUP BY {colq} HAVING COUNT(*) = 1))", False
        return f"{colq} IN (SELECT {colq} FROM {_q(fqn)} GROUP BY {colq} HAVING COUNT(*) = 1)", False
    if ctype == "NULL_COUNT":
        # failures are rows where NOT(predicate) -> predicate should be "col IS NOT NULL" if max_nulls=0
        return f"{colq} IS NOT NULL", False
    if ctype == "MIN_MAX":
        min_v = params.get("min"); max_v = params.get("max")
        conds = []
        if min_v not in (None, ""): conds.append(f"{colq} >= {min_v}")
        if max_v not in (None, ""): conds.append(f"{colq} <= {max_v}")
        return "(" + " AND ".join(conds or ["TRUE"]) + ")", False
    if ctype == "WHITESPACE":
        mode = params.get("mode", "NO_LEADING_TRAILING")
        if mode == "NO_LEADING_TRAILING":
            return f"({colq} IS NULL OR {colq} = TRIM({colq}))", False
        if mode == "NO_INTERNAL_ONLY_WHITESPACE":
            return f"({colq} IS NULL OR REGEXP_REPLACE({colq}, '\\\\s+', ' ') = {colq})", False
        return f"({colq} IS NOT NULL AND LENGTH(TRIM({colq})) > 0)", False
    if ctype == "FORMAT_DISTRIBUTION":
        regex = params.get("regex", ".*")
        return f"({colq} IS NULL OR {colq} RLIKE '{regex}')", False
    if ctype == "VALUE_DISTRIBUTION":
        allowed_csv = params.get("allowed_values_csv", "")
        values = [v.strip() for v in allowed_csv.split(",") if v.strip() != ""]
        if not values: return "(TRUE)", False
        quoted_values = []
        for raw in values:
            sanitized = raw.replace("'", "''")
            quoted_values.append("'" + sanitized + "'")
        quoted = ", ".join(quoted_values)
        return f"({colq} IN ({quoted}))", False
    return "(TRUE)", False

def build_rule_for_table_check(fqn: str, ttype: str, params: dict) -> str:
    ttype = (ttype or "").upper()
    if ttype == "FRESHNESS":
        ts_col = params.get("timestamp_column", "LOAD_TIMESTAMP")
        max_age = int(params.get("max_age_minutes", 720))
        return f"SELECT TIMESTAMPDIFF('minute', MAX(\"{ts_col}\"), CURRENT_TIMESTAMP()) <= {max_age} AS OK FROM {_q(fqn)}"
    if ttype == "ROW_COUNT":
        min_rows = int(params.get("min_rows", 1))
        return f"SELECT COUNT(*) >= {min_rows} AS OK FROM {_q(fqn)}"
    return "SELECT TRUE AS OK"
