"""Unit tests for retail data cleaning and validation."""

import pandas as pd

from src.transformation import clean_retail_data


def sample_data():
    """Return a small dataset covering key business rules."""
    return pd.DataFrame(
        {
            "InvoiceNo": ["1001", "1002", "1003", "1004"],
            "StockCode": [" abc ", "xyz", "def", "ghi"],
            "Description": ["Product A", None, "Product C", "Product D"],
            "Quantity": [2, -1, 3, 1],
            "InvoiceDate": [
                "12/1/2010 8:26",
                "12/2/2010 9:30",
                "12/3/2010 10:15",
                "12/4/2010 11:00",
            ],
            "UnitPrice": [10.0, 5.0, 0.0, 8.0],
            "CustomerID": ["123", None, "456", "789"],
            "Country": ["United Kingdom"] * 4,
        }
    )


def test_invalid_price_is_rejected():
    accepted, rejected = clean_retail_data(sample_data())

    assert len(rejected) == 1
    assert rejected.iloc[0]["InvoiceNo"] == "1003"
    assert "invalid_unit_price" in rejected.iloc[0]["rejection_reason"]


def test_returns_are_preserved_and_classified():
    accepted, _ = clean_retail_data(sample_data())

    returned = accepted.loc[accepted["InvoiceNo"] == "1002"].iloc[0]

    assert returned["transaction_type"] == "return_or_adjustment"
    assert returned["gross_revenue"] == -5.0


def test_product_codes_are_normalized():
    accepted, _ = clean_retail_data(sample_data())

    assert accepted.iloc[0]["StockCode"] == "ABC"


def test_missing_customer_and_description_are_preserved():
    accepted, _ = clean_retail_data(sample_data())

    returned = accepted.loc[accepted["InvoiceNo"] == "1002"].iloc[0]

    assert pd.isna(returned["CustomerID"])
    assert pd.isna(returned["Description"])


def test_source_dataframe_is_not_modified():
    original = sample_data()
    original_copy = original.copy(deep=True)

    clean_retail_data(original)

    pd.testing.assert_frame_equal(original, original_copy)