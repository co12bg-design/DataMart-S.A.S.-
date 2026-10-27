"""This script is read-only: it does not modify or delete source data.
It generates a quality report for each CSV file in data/raw/.
"""

from pathlib import Path

import pandas as pd


# Resolve paths from the project root, not from the current terminal folder.
PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw"
REPORT_DIR = PROJECT_ROOT / "reports"

SOURCE_FILES = [
    RAW_DATA_DIR / "data.csv",
    RAW_DATA_DIR / "online_retail.csv"
    ]


def profile_dataset(csv_path: Path) -> list[str]:
    """Inspect structure, missing values, duplicates, and business rules."""
    print(f"\n{'=' * 65}")
    print(f"DATASET: {csv_path.name}")
    print("=" * 65)

    # Try common encodings used by exported retail CSV files.
    # The original CSV is never modified.
    encodings = ("utf-8", "cp1252", "latin1")

    for encoding in encodings:
        try:
            df = pd.read_csv(
                csv_path,
                low_memory=False,
                encoding=encoding,
                )
            print(f"Encoding used: {encoding}")
            break
        except UnicodeDecodeError:
            continue
    else:
        raise ValueError(
        f"Unable to decode CSV file: {csv_path.name}"
    )

    lines = [
        f"File: {csv_path.name}",
        f"Rows: {len(df):,}",
        f"Columns: {len(df.columns)}",
        f"Exact duplicate rows: {df.duplicated().sum():,}",
        f"Unique product codes: {df['StockCode'].nunique(dropna=True):,}",
        f"Unique countries: {df['Country'].nunique(dropna=True):,}",
    ]

    # Report missing values for every column.
    lines.append("\nMissing values by column:")
    lines.extend(
        f"  - {column}: {count:,}"
        for column, count in df.isna().sum().items()
    )

    # Convert numeric fields only for analysis; keep the original dataframe intact.
    quantity = pd.to_numeric(df["Quantity"], errors="coerce")
    price = pd.to_numeric(df["UnitPrice"], errors="coerce")
    # Parse dates using the source format: month/day/year.
    parsed_dates = pd.to_datetime(
        df["InvoiceDate"],
        format="%m/%d/%Y %H:%M",
        errors="coerce",
    )

    invalid_dates = parsed_dates.isna().sum()

    print(f"Invalid invoice dates after parsing: {invalid_dates:,}")
    print("Sample parsed dates:")
    print(parsed_dates.dropna().head(10).to_string(index=False))

    print("\nData type of InvoiceDate:", df["InvoiceDate"].dtype)
    print("\nMost frequent original date values:")
    print(df["InvoiceDate"].astype(str).value_counts().head(10).to_string())

    invalid_dates = parsed_dates.isna().sum()

    print(f"Invalid invoice dates after parsing: {invalid_dates:,}")
    print("Sample original dates:")
    print(df["InvoiceDate"].dropna().head(10).to_string(index=False))
    print("Sample parsed dates:")
    print(parsed_dates.dropna().head(10).to_string(index=False))

    lines.extend(
        [
            f"\nQuantity <= 0: {(quantity <= 0).sum():,}",
            f"Invalid quantity values: {quantity.isna().sum():,}",
            f"Unit price <= 0: {(price <= 0).sum():,}",
            f"Invalid price values: {price.isna().sum():,}",
            f"Invalid invoice dates: {invalid_dates:,}",
            f"Earliest valid date: {parsed_dates.min()}",
            f"Latest valid date: {parsed_dates.max()}",
        ]
    )

    report = "\n".join(lines)
    print(report)
    return [report]


def main() -> None:
    """Profile every CSV found in the raw data directory."""
    if not RAW_DATA_DIR.exists():
        raise FileNotFoundError(
            f"Raw data directory not found: {RAW_DATA_DIR}"
        )

    csv_files = sorted(RAW_DATA_DIR.glob("*.csv"))
    if not csv_files:
        raise FileNotFoundError(
            f"No CSV files found in {RAW_DATA_DIR}"
        )

    # Create the report directory only when the script is run.
    REPORT_DIR.mkdir(parents=True, exist_ok=True)

    reports = []
    for csv_path in csv_files:
        reports.extend(profile_dataset(csv_path))

    report_path = REPORT_DIR / "data_quality_report.txt"
    report_path.write_text("\n\n".join(reports), encoding="utf-8")
    print(f"\nReport saved to: {report_path}")


if __name__ == "__main__":
    main()