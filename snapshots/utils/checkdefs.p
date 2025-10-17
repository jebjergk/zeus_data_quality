SUPPORTED_COLUMN_CHECKS = ["UNIQUE","NULL_COUNT","MIN_MAX","WHITESPACE","FORMAT_DISTRIBUTION","VALUE_DISTRIBUTION"]
SUPPORTED_TABLE_CHECKS  = ["FRESHNESS","ROW_COUNT","ROW_COUNT_ANOMALY"]


_WRAPPING_DELIMS = {
    '"': '"',
    "'": "'",
    '`': '`',
    '[': ']',
}


def _strip_wrapping_delimiters(value: str) -> str:
    text = (value or "").strip()
    while len(text) >= 2:
        start = text[0]
        end = text[-1]
        match = _WRAPPING_DELIMS.get(start)
        if match and end == match:
            text = text[1:-1].strip()
            continue
        break
    return text


def _sanitize_identifier(value: str) -> str:
    text = _strip_wrapping_delimiters(value)
    if not text:
        raise ValueError("Identifier is required")
    if '.' in text:
        raise ValueError("Identifier must not include '.' characters")
    for forbidden in (';', '--', '/*', '*/', '\n', '\r'):
        if forbidden in text:
            raise ValueError("Identifier contains invalid characters")
    text = text.replace('"', '""')
    if not text:
        raise ValueError("Identifier is required")
    return text


def _quote_identifier(value: str) -> str:
    return f'"{_sanitize_identifier(value)}"'


def _quote_table_fqn(fqn: str) -> str:
    parts = [p for p in (fqn or "").split('.') if p.strip()]
    if len(parts) != 3:
        raise ValueError("Table FQN must be in the form DB.SCHEMA.TABLE")
    return '.'.join(_quote_identifier(part) for part in parts)


def _q(ident: str) -> str:
    parts = [p for p in (ident or "").split('.') if p.strip()]
    if not parts:
        raise ValueError("Identifier is required")
    return '.'.join(_quote_identifier(part) for part in parts)


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
            return f"({colq} IS NULL OR REGEXP_REPLACE({colq}, '\\s+', ' ') = {colq})", False
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


def build_rule_for_table_check(fqn: str, ttype: str, params: dict):
    ttype = (ttype or "").upper()
    if ttype == "FRESHNESS":
        ts_col_raw = params.get("timestamp_column", "LOAD_TIMESTAMP")
        ts_col = _quote_identifier(ts_col_raw)
        max_age = int(params.get("max_age_minutes", 1920))
        table_name = _quote_table_fqn(fqn)
        return "\n".join([
            "SELECT (COUNT(*) > 0 AND COUNT({col}) > 0 AND".format(col=ts_col),
            "        TIMESTAMPDIFF('minute', MAX({col}), CURRENT_TIMESTAMP()) <= {max_age}) AS OK".format(col=ts_col, max_age=max_age),
            f"FROM {table_name}",
        ]), True
    if ttype == "ROW_COUNT":
        min_rows = int(params.get("min_rows", 1))
        table_name = _quote_table_fqn(fqn)
        return f"SELECT COUNT(*) >= {min_rows} AS OK FROM {table_name}", True
    if ttype == "ROW_COUNT_ANOMALY":
        ts_col_raw = params.get("timestamp_column", "LOAD_TIMESTAMP")
        ts_col = _quote_identifier(ts_col_raw)
        lookback_days = int(params.get("lookback_days", 28))
        sensitivity = float(params.get("sensitivity", 3.0))
        min_history_days = int(params.get("min_history_days", 7))
        table_name = _quote_table_fqn(fqn)
        return "\n".join([
            "WITH history AS (",
            f"    SELECT DATE_TRUNC('day', {ts_col}) AS day, COUNT(*) AS c",
            f"    FROM {table_name}",
            f"    WHERE {ts_col} IS NOT NULL",
            f"      AND {ts_col} >= DATEADD('day', -{lookback_days}, CURRENT_DATE)",
            f"      AND DATE({ts_col}) < CURRENT_DATE",
            "    GROUP BY 1",
            "), aggregates AS (",
            "    SELECT",
            "        COUNT(*) AS history_days,",
            "        APPROX_PERCENTILE(c, 0.5) AS median_c",
            "    FROM history",
            "), mad_calc AS (",
            "    SELECT",
            "        APPROX_PERCENTILE(ABS(h.c - agg.median_c), 0.5) AS mad",
            "    FROM history h",
            "    CROSS JOIN aggregates agg",
            "), today AS (",
            f"    SELECT COUNT(*) AS c_today",
            f"    FROM {table_name}",
            f"    WHERE {ts_col} IS NOT NULL",
            f"      AND DATE({ts_col}) = CURRENT_DATE",
            ")",
            "SELECT (",
            f"    aggregates.history_days >= {min_history_days}",
            f"    AND COALESCE(ABS(today.c_today - aggregates.median_c) / NULLIF(1.4826 * mad_calc.mad, 0) <= {sensitivity}, FALSE)",
            ") AS OK",
            "FROM aggregates",
            "CROSS JOIN mad_calc",
            "CROSS JOIN today",
        ]), True
    return "SELECT TRUE AS OK", True
