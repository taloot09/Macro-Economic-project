import pandas as pd
from typing import Iterable

def _try_parse_dates(series: pd.Series) -> pd.Series:
    """
    Try multiple parsing strategies to convert month-like strings (e.g. 'Jul-13') or
    full dates into pandas.Timestamp. Returns a datetime64[ns] Series (NaT where parse fails).
    """
    # 1) Try common short-month formats like Jul-13, Jul-2013
    formats = ["%b-%y", "%b-%Y", "%Y-%m-%d", "%Y-%m"]
    for fmt in formats:
        try:
            parsed = pd.to_datetime(series, format=fmt, errors="coerce")
            # if sufficiently many parsed, return
            if parsed.notna().sum() > 0:
                return parsed
        except Exception:
            pass
    # 2) Fallback to general parser (dateutil)
    return pd.to_datetime(series, errors="coerce")

def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """Clean raw wide-format data and convert to long-format with fiscal year assignment.

    Accepts either:
      - wide format: columns = Description, Jul-13, Aug-13, ...
      - long format: columns = description, date, value

    Returns long-format DataFrame with columns: description, date (datetime), value, fiscal_year
    """

    df = df.copy()

    # Standardize column names (keep original 'Description' case-insensitive)
    df.columns = [c.strip() for c in df.columns]

    # Normalize the 'Description' column name to lower-case internal name
    desc_col = None
    for c in df.columns:
        if c.strip().lower() == "description":
            desc_col = c
            break

    if desc_col is None:
        raise ValueError("Input must contain a 'Description' column (case-insensitive).")

    # If data is wide-format (many non-description columns that look like month labels), melt it.
    other_cols = [c for c in df.columns if c != desc_col]
    # Heuristic: if there are many other columns and they look like month-year labels, melt.
    if len(other_cols) > 1:
        # Perform melt into long format
        df_long = df.melt(id_vars=[desc_col], var_name="date", value_name="value")
        df_long = df_long.rename(columns={desc_col: "description"})
    else:
        # Assume already long format and attempt to rename columns to standard names
        df_long = df.rename(columns={desc_col: "description"})
        # if date/value columns exist with different cases, normalize:
        if "date" not in df_long.columns:
            # try to detect likely date column name
            for c in df_long.columns:
                if "date" in c.lower():
                    df_long = df_long.rename(columns={c: "date"})
                    break
        if "value" not in df_long.columns:
            for c in df_long.columns:
                if c.lower() in ("value", "val", "amount"):
                    df_long = df_long.rename(columns={c: "value"})
                    break

    # Trim whitespace in description and coerce value to numeric
    df_long["description"] = df_long["description"].astype(str).str.strip()

    # Parse date column robustly (handles 'Jul-13' etc.)
    df_long["date_raw"] = df_long["date"].astype(str).str.strip()
    df_long["date"] = _try_parse_dates(df_long["date_raw"])

    # Drop rows where date could not be parsed (these would break downstream pivot)
    unparsed_count = df_long["date"].isna().sum()
    if unparsed_count > 0:
        # It may be better to keep or log instead of dropping; here we drop because pivot requires clean dates
        df_long = df_long[df_long["date"].notna()].copy()

    # Ensure numeric values
    df_long["value"] = pd.to_numeric(df_long["value"], errors="coerce")

    # Drop rows where value is NaN (optional: keep as 0 or impute instead)
    df_long = df_long.dropna(subset=["value"])

    # Aggregate duplicates if any (same description + date) by summing values.
    # This prevents pivot errors when duplicates exist.
    df_long = df_long.groupby(["date", "description"], as_index=False).agg({"value": "sum"})

    # Sort
    df_long = df_long.sort_values(["date", "description"]).reset_index(drop=True)

    # Assign fiscal year: FY starts July 1. Convention: FY year = calendar year when year ends.
    # For example: 2013-07-01 ... 2014-06-30 => fiscal_year = 2014
    df_long["fiscal_year"] = df_long["date"].apply(lambda d: d.year if d.month >= 7 else d.year - 1)

    print(f"📌 Parsed dates; dropped {unparsed_count} unparsable rows (if any).")
    print("📌 Fiscal year assigned successfully.")
    print("✅ Data cleaned & reshaped successfully.")
    return df_long
