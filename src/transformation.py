"""Data cleaning and transformation rules for the DataMart ETL."""

import pandas as pd

# Original date format in the source CSV files.
DATE_FORMAT = "%m/%d/%Y %H:%M"


def clean_retail_data(
    df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Clean retail transactions and return accepted and rejected records.

    The original DataFrame is not modified.
    Rejected records include a reason for auditing.
    """
    data = df.copy()

    # Normalize column names and product codes.
    data.columns = data.columns.str.strip()

    required_columns = [
        "InvoiceNo",
        "StockCode",
        "Quantity",
        "InvoiceDate",
        "UnitPrice",
    ]

    missing_columns = [
        column for column in required_columns
        if column not in data.columns
    ]

    if missing_columns:
        raise ValueError(
            f"Missing required columns: {missing_columns}"
        )

    data["StockCode"] = (
        data["StockCode"].astype("string").str.strip().str.upper()
    )
    data["InvoiceNo"] = data["InvoiceNo"].astype("string").str.strip()

    # Parse dates and numeric fields safely.
    data["InvoiceDate"] = pd.to_datetime(
        data["InvoiceDate"],
        format=DATE_FORMAT,
        errors="coerce",
    )
    data["Quantity"] = pd.to_numeric(
        data["Quantity"], errors="coerce"
    )
    data["UnitPrice"] = pd.to_numeric(
        data["UnitPrice"], errors="coerce"
    )

    # Normalize text without replacing missing values with fake data.
    if "Description" in data.columns:
        data["Description"] = (
            data["Description"].astype("string").str.strip()
        )

    if "Country" in data.columns:
        data["Country"] = data["Country"].astype("string").str.strip()

    # Identify invalid records.
    invalid_invoice = (
        data["InvoiceNo"].isna() | data["InvoiceNo"].eq("")
    )
    invalid_product = (
        data["StockCode"].isna() | data["StockCode"].eq("")
    )
    invalid_date = data["InvoiceDate"].isna()
    invalid_quantity = (
        data["Quantity"].isna()
        | (data["Quantity"] % 1 != 0)
    )
    invalid_price = (
        data["UnitPrice"].isna() | (data["UnitPrice"] <= 0)
    )

    reject_mask = (
        invalid_invoice
        | invalid_product
        | invalid_date
        | invalid_quantity
        | invalid_price
    )

    # Keep every applicable rejection reason for auditing.
    rejected = data.loc[reject_mask].copy()
    rejected["rejection_reason"] = ""

    rejection_rules = [
        (invalid_invoice, "missing_invoice_number"),
        (invalid_product, "missing_product_code"),
        (invalid_date, "invalid_invoice_date"),
        (invalid_quantity, "invalid_quantity"),
        (invalid_price, "invalid_unit_price"),
    ]

    for mask, reason in rejection_rules:
        rejected.loc[mask.loc[reject_mask], "rejection_reason"] += (
            reason + ";"
        )

    # Keep valid records and classify sales and returns.
    accepted = data.loc[~reject_mask].copy()
    accepted["Quantity"] = accepted["Quantity"].astype("int64")

    accepted["transaction_type"] = accepted["Quantity"].apply(
        lambda quantity: (
            "sale" if quantity > 0 else "return_or_adjustment"
        )
    )

    # Calculate line values. Returns remain visible as negative values.
    accepted["gross_revenue"] = (
        accepted["Quantity"] * accepted["UnitPrice"]
    ).round(2)

    # The source has no timezone metadata; timestamps are assumed to be UTC.
    accepted["InvoiceDate"] = accepted["InvoiceDate"].dt.tz_localize(
        "UTC"
    )

    return accepted, rejected