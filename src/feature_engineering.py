import pandas as pd
from typing import Dict, Optional

def _normalize_text(s: str) -> str:
    return "".join(ch.lower() if ch.isalnum() or ch.isspace() else " " for ch in str(s)).strip()

def _build_column_map(cols: pd.Index) -> Dict[str, str]:
    mapping = {}
    normalized_to_col = { _normalize_text(c): c for c in cols }

    keywords = {
        "exports": ["export", "exports", "exports_of_goods", "exports_of_goods_fob"],
        "imports": ["import", "imports", "imports_of_goods", "imports_of_goods_fob"],
        "services_export": ["services export", "services_exports", "services_export"],
        "services_import": ["services import", "services_imports", "services_import"],
        "pi_credit": ["primary income credit", "primary income: credit", "pi credit", "pi_credit"],
        "pi_debit": ["primary income debit", "primary income: debit", "pi debit", "pi_debit"],
        "secondary_credit": ["secondary income credit", "secondary credit", "secondary_income_credit"],
        "secondary_debit": ["secondary income debit", "secondary debit", "secondary_income_debit"],
        "workers_remittances": ["workers remittances", "workers' remittances", "workers_remittances", "remittances"],
        "current_account_balance": ["current account balance", "current_account_balance", "current_account"],
        "gdp": ["gdp", "gross_domestic_product", "gdp_current_prices", "gdp_current"]
    }

    for canon, kw_list in keywords.items():
        found = None
        for norm_col, orig_col in normalized_to_col.items():
            for kw in kw_list:
                if kw in norm_col:
                    found = orig_col
                    break
            if found:
                break
        if found:
            mapping[canon] = found

    return mapping

def create_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """
    Create full Current Account hierarchy indicators from long-format df:
    expected columns: ['description', 'date', 'value', 'fiscal_year' (optional)]
    """

    df = df.copy()

    # Validate presence of columns
    if not {"description", "date", "value"}.issubset(set(df.columns)):
        raise ValueError("Input df must include 'description', 'date', and 'value' columns")

    # Ensure date is datetime
    if not pd.api.types.is_datetime64_any_dtype(df["date"]):
        df["date"] = pd.to_datetime(df["date"], errors="coerce")

    # Remove duplicates or aggregate if duplicates exist (safety)
    df = df.groupby(["date", "description"], as_index=False).agg({"value": "sum"})

    # Pivot wide for calculations (rows = date, cols = descriptions)
    pivot = df.pivot(index="date", columns="description", values="value")

    # Build mapping of canonical names to actual pivot columns
    col_map = _build_column_map(pivot.columns)

    def col(canon: str) -> Optional[pd.Series]:
        return pivot[col_map[canon]] if canon in col_map else None

    # 1) Balance on Goods
    if col("exports") is not None and col("imports") is not None:
        pivot["balance_on_goods"] = col("exports") - col("imports")

    # 2) Balance on Services
    if col("services_export") is not None and col("services_import") is not None:
        pivot["balance_on_services"] = col("services_export") - col("services_import")

    # 3) Balance on Primary Income
    if col("pi_credit") is not None and col("pi_debit") is not None:
        pivot["balance_on_primary_income"] = col("pi_credit") - col("pi_debit")

    # 4) Balance on Secondary Income (include remittances)
    sec_credit = col("secondary_credit")
    remits = col("workers_remittances")
    sec_debit = col("secondary_debit")

    if sec_credit is not None and remits is not None:
        pivot["secondary_credit_combined"] = sec_credit.fillna(0) + remits.fillna(0)
    elif sec_credit is not None:
        pivot["secondary_credit_combined"] = sec_credit.fillna(0)
    elif remits is not None:
        pivot["secondary_credit_combined"] = remits.fillna(0)

    if "secondary_credit_combined" in pivot.columns and sec_debit is not None:
        pivot["balance_on_secondary_income"] = pivot["secondary_credit_combined"] - sec_debit

    # 5) Aggregate Current Account from available balances
    comps = ["balance_on_goods", "balance_on_services", "balance_on_primary_income", "balance_on_secondary_income"]
    available = [c for c in comps if c in pivot.columns]
    if available:
        pivot["current_account_calculated"] = pivot[available].sum(axis=1)

    # If reported current account exists, preserve and compute diff
    if "current_account_balance" in col_map:
        orig_name = col_map["current_account_balance"]
        pivot.rename(columns={orig_name: "current_account_reported"}, inplace=True)
        if "current_account_calculated" in pivot.columns:
            pivot["current_account_diff"] = pivot["current_account_reported"] - pivot["current_account_calculated"]

    # 6) CA percent of GDP if available
    if "current_account_calculated" in pivot.columns and "gdp" in col_map:
        gdp_series = pivot[col_map["gdp"]]
        pivot["ca_percent_gdp"] = (pivot["current_account_calculated"] / gdp_series.replace({0: pd.NA})) * 100

    # Meta flags for downstream logic
    pivot["_has_balance_on_goods"] = "balance_on_goods" in pivot.columns
    pivot["_has_balance_on_services"] = "balance_on_services" in pivot.columns
    pivot["_has_balance_on_primary_income"] = "balance_on_primary_income" in pivot.columns
    pivot["_has_balance_on_secondary_income"] = "balance_on_secondary_income" in pivot.columns
    pivot["_has_current_account_calculated"] = "current_account_calculated" in pivot.columns

    # Reset index and convert back to long format
    df_long = pivot.reset_index().melt(id_vars="date", var_name="description", value_name="value")

    # Ensure description column is string and date is datetime
    df_long["description"] = df_long["description"].astype(str)
    df_long["date"] = pd.to_datetime(df_long["date"])

    print("✅ Full-hierarchy indicators created successfully.")
    return df_long
