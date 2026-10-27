"""Compare the two retail CSV sources without modifying either file."""

from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = ROOT / "data" / "raw"

SOURCE_1 = RAW_DIR / "data.csv"
SOURCE_2 = RAW_DIR / "online_retail.csv"

COLUMNS = [
    "InvoiceNo",
    "StockCode",
    "Description",
    "Quantity",
    "InvoiceDate",
    "UnitPrice",
    "CustomerID",
    "Country",
]


def main():
    """Compare records using the shared business columns."""
    for path in (SOURCE_1, SOURCE_2):
        if not path.exists():
            raise FileNotFoundError(f"Source file not found: {path}")

    first = pd.read_csv(SOURCE_1, encoding="cp1252", low_memory=False)
    second = pd.read_csv(SOURCE_2, encoding="utf-8", low_memory=False)

    first = first[COLUMNS].copy()
    second = second[COLUMNS].copy()

    # Compare the number of records and the shared-column schemas.
    print(f"Source 1 rows: {len(first):,}")
    print(f"Source 2 rows: {len(second):,}")
    print(f"Same column names: {list(first.columns) == list(second.columns)}")

    # Sorting makes the comparison independent of row order.
    first_sorted = first.sort_values(COLUMNS, na_position="first").reset_index(drop=True)
    second_sorted = second.sort_values(COLUMNS, na_position="first").reset_index(drop=True)

    print(f"Identical records after sorting: {first_sorted.equals(second_sorted)}")
    print(f"Source 1 duplicate business rows: {first.duplicated().sum():,}")
    print(f"Source 2 duplicate business rows: {second.duplicated().sum():,}")


if __name__ == "__main__":
    main()