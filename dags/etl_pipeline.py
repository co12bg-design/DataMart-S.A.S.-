"""Reusable extraction and transformation functions for DataMart S.A.S.

Responsibilities:
1. Load CSV files.
2. Normalize source column names.
3. Validate retail transactions.
4. Separate valid and rejected records.
5. Deduplicate transactions across sources.
6. Prepare fact and product dimension DataFrames.
"""

from __future__ import annotations

import hashlib
import json
import logging
from pathlib import Path
from typing import Any

import pandas as pd


LOG = logging.getLogger(__name__)

DATE_FORMAT = "%m/%d/%Y %H:%M"

# Required fields for transaction processing.
REQUIRED_COLUMNS = {
    "invoice",
    "product_code",
    "quantity",
    "invoice_date",
    "unit_price",
}

# Maps source-specific column names to canonical names.
COLUMN_ALIASES = {
    "invoiceno": "invoice",
    "invoice_no": "invoice",
    "invoice": "invoice",
    "stockcode": "product_code",
    "stock_code": "product_code",
    "product_code": "product_code",
    "description": "description",
    "quantity": "quantity",
    "invoicedate": "invoice_date",
    "invoice_date": "invoice_date",
    "price": "unit_price",
    "unitprice": "unit_price",
    "unit_price": "unit_price",
    "customerid": "customer_id",
    "customer_id": "customer_id",
    "country": "country",
}


def stable_hash(value: Any) -> str:
    """Return a deterministic SHA-256 hash for a value."""
    serialized = json.dumps(
        value,
        sort_keys=True,
        ensure_ascii=False,
        default=str,
    )
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def load_csv(
    path: str | Path,
    source: str | None = None,
    ) -> pd.DataFrame:
    """Load a CSV as strings to preserve original source values."""
    file_path = Path(path)

    if not file_path.is_file():
        raise FileNotFoundError(
            f"Source file does not exist: {file_path}"
        )

    dataframe = pd.read_csv(
        file_path,
        dtype=str,
        encoding_errors="replace",
    )

    if source is not None:
        dataframe["source_name"] = source

    LOG.info("Loaded %s rows from %s", len(dataframe), file_path)
    return dataframe


def resolve_input(
    primary: str | Path,
    fallback: str | Path | None = None,
) -> Path:
    """Return the first existing path or raise a clear error."""
    primary_path = Path(primary)

    if primary_path.is_file():
        return primary_path

    if fallback is not None:
        fallback_path = Path(fallback)

        if fallback_path.is_file():
            LOG.warning("Using fallback input: %s", fallback_path)
            return fallback_path

    raise FileNotFoundError(
        f"No input file found. Primary: {primary_path}; "
        f"fallback: {fallback}"
    )


def normalize_columns(
    dataframe: pd.DataFrame,
    source_name: str,
) -> pd.DataFrame:
    """Normalize column names and add source lineage."""
    normalized = dataframe.copy()

    # Remove surrounding spaces and normalize capitalization.
    normalized.columns = [
        str(column).strip().lower()
        for column in normalized.columns
    ]

    # Ignore spaces in source headers, such as "Customer ID".
    rename_mapping = {
        column: COLUMN_ALIASES[column.replace(" ", "").lower()]
        for column in normalized.columns
        if column.replace(" ", "").lower() in COLUMN_ALIASES
    }
    normalized = normalized.rename(columns=rename_mapping)

    # Fail early when a required business field is missing.
    missing_columns = REQUIRED_COLUMNS - set(normalized.columns)

    if missing_columns:
        missing = ", ".join(sorted(missing_columns))
        raise ValueError(
            f"Source '{source_name}' is missing required columns: "
            f"{missing}"
        )

    # Optional fields must exist in both sources for concatenation.
    for column in ("description", "customer_id", "country"):
        if column not in normalized.columns:
            normalized[column] = pd.NA

    normalized["source_name"] = source_name
    return normalized


def _parse_dates(values: pd.Series) -> pd.Series:
    """Parse the source date format and support ISO timestamps."""
    text_values = values.astype("string").str.strip()

    # Parse the original MM/DD/YYYY HH:MM format first.
    parsed_dates = pd.to_datetime(
        text_values,
        format=DATE_FORMAT,
        errors="coerce",
    )

    # Retry values that use a different, typically ISO, format.
    remaining = parsed_dates.isna() & text_values.notna()

    if remaining.any():
        iso_dates = pd.to_datetime(
            text_values.loc[remaining],
            format="ISO8601",
            errors="coerce",
        )

        parsed_dates.loc[remaining] = iso_dates

    # Localize naive timestamps or convert aware timestamps to UTC.
    if parsed_dates.dt.tz is None:
        parsed_dates = parsed_dates.dt.tz_localize("UTC")
    else:
        parsed_dates = parsed_dates.dt.tz_convert("UTC")

    return parsed_dates


def transform_sources(
    source_one: pd.DataFrame,
    source_two: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Transform two sources into facts, products, and rejected records."""
    first = normalize_columns(source_one, "source_one")
    second = normalize_columns(source_two, "source_two")

    combined = pd.concat(
        [first, second],
        ignore_index=True,
    )

    # Standardize text fields without turning missing values into "nan".
    text_columns = (
        "invoice",
        "product_code",
        "description",
        "customer_id",
        "country",
    )

    for column in text_columns:
        combined[column] = (
            combined[column].astype("string").str.strip()
        )
        combined[column] = combined[column].replace("", pd.NA)

    # Product identifiers must be consistent across both sources.
    combined["product_code"] = (
        combined["product_code"]
        .str.upper()
        .str.replace(r"\s+", "", regex=True)
    )

    # Convert numeric fields; invalid values become missing values.
    combined["quantity"] = pd.to_numeric(
        combined["quantity"],
        errors="coerce",
    )
    combined["unit_price"] = pd.to_numeric(
        combined["unit_price"],
        errors="coerce",
    )
    combined["transaction_ts"] = _parse_dates(
        combined["invoice_date"]
    )

    # Accumulate all applicable validation reasons per row.
    reasons = pd.Series(
        "",
        index=combined.index,
        dtype="string",
    )

    def add_reason(mask: pd.Series, reason: str) -> None:
        """Append a validation reason to selected rows."""
        selected = mask.fillna(True)
        existing = reasons.loc[selected]

        reasons.loc[selected] = existing.map(
            lambda current: (
                f"{current}; {reason}" if current else reason
            )
        )

    add_reason(combined["invoice"].isna(), "missing_invoice")
    add_reason(
        combined["product_code"].isna(),
        "missing_product_code",
    )
    add_reason(
        combined["transaction_ts"].isna(),
        "invalid_invoice_date",
    )
    add_reason(
        combined["quantity"].isna(),
        "invalid_quantity",
    )
    add_reason(
        combined["unit_price"].isna(),
        "invalid_unit_price",
    )
    add_reason(
        combined["unit_price"].le(0),
        "unit_price_must_be_positive",
    )

    # Fractional quantities are not valid for this retail dataset.
    fractional_quantity = (
        combined["quantity"].notna()
        & combined["quantity"].mod(1).ne(0)
    )
    add_reason(fractional_quantity, "fractional_quantity")

    # Returns have negative quantities and remain valid transactions.
    rejected_mask = reasons.ne("")
    rejected = combined.loc[rejected_mask].copy()
    rejected["reason"] = reasons.loc[rejected_mask]
    rejected["rejection_reason"] = rejected["reason"]

    valid = combined.loc[~rejected_mask].copy()

    # Handle an empty valid dataset without applying operations to rows.
    if valid.empty:
        valid["quantity"] = pd.Series(
            index=valid.index,
            dtype="int64",
        )
    else:
        valid["quantity"] = valid["quantity"].astype("int64")

    # Classify sales and returns.
    valid["transaction_type"] = "SALE"
    valid.loc[
        valid["quantity"] < 0,
        "transaction_type",
    ] = "RETURN"

    # Keep sales and return values separate for reconciliation.
    valid["gross_revenue"] = (
        valid["quantity"].clip(lower=0) * valid["unit_price"]
    )
    valid["return_amount"] = (
        valid["quantity"].clip(upper=0).abs()
        * valid["unit_price"]
    )
    valid["net_revenue"] = (
        valid["gross_revenue"] - valid["return_amount"]
    )

    # The business key excludes source name and description so equivalent
    # records from different sources can be identified as duplicates.
    if not valid.empty:
        key_columns = [
            "invoice",
            "product_code",
            "transaction_ts",
            "quantity",
            "unit_price",
            "customer_id",
        ]

        valid["transaction_key"] = valid.apply(
            lambda row: stable_hash(
                {
                    column: (
                        None
                        if pd.isna(row[column])
                        else str(row[column])
                    )
                    for column in key_columns
                }
            ),
            axis=1,
        )

        valid = valid.drop_duplicates(
            subset=["transaction_key"],
            keep="first",
        )
    else:
        valid["transaction_key"] = pd.Series(
            index=valid.index,
            dtype="string",
        )

    # Build one product record per normalized product code.
    if valid.empty:
        products = pd.DataFrame(
            columns=["product_code", "description"]
        )
    else:
        products = (
            valid[["product_code", "description"]]
            .dropna(subset=["product_code"])
            .drop_duplicates(
                subset=["product_code"],
                keep="first",
            )
            .reset_index(drop=True)
        )

    # Use the fact-table invoice field name consistently.
    facts = valid.rename(
        columns={"invoice": "invoice_no"}
    ).reset_index(drop=True)

    rejected = rejected.rename(
        columns={"invoice": "invoice_no"}
    ).reset_index(drop=True)

    LOG.info(
        "ETL transformation completed: %s valid, %s rejected, "
        "%s products",
        len(facts),
        len(rejected),
        len(products),
    )

    return facts, products, rejected