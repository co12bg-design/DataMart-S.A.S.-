"""Unit tests for critical DataMart S.A.S. ETL transformation rules."""

import pandas as pd

from dags.etl_pipeline import transform_sources


def make_record(
    invoice: str,
    product: str = "P01",
    quantity: str = "1",
    price: str = "5",
    date: str = "12/1/2010 8:26",
    customer: str = "100",
) -> dict[str, str]:
    """Create a standard source record for test cases."""
    return {
        "InvoiceNo": invoice,
        "StockCode": product,
        "Description": "Sample item",
        "Quantity": quantity,
        "InvoiceDate": date,
        "UnitPrice": price,
        "CustomerID": customer,
        "Country": "UK",
    }


def test_returns_are_separated_and_net_revenue_is_negative():
    """Sales and returns must have separate amounts."""
    one = pd.DataFrame(
        [
            {
                **make_record("A1", product=" x1 ", quantity="2"),
                "UnitPrice": "5",
            },
            {
                **make_record("A2", product="X1", quantity="-1"),
                "UnitPrice": "5",
            },
        ]
    )

    two = pd.DataFrame(
        [
            {
                "Invoice": "A3",
                "StockCode": "X2",
                "Description": "Thing",
                "Quantity": "1",
                "InvoiceDate": "2010-12-01 09:00:00",
                "Price": "0",
                "Customer ID": "20",
                "Country": "UK",
            }
        ]
    )

    facts, products, rejects = transform_sources(one, two)

    assert len(facts) == 2

    sale = facts.loc[
        facts["transaction_type"] == "SALE"
    ].iloc[0]

    returned = facts.loc[
        facts["transaction_type"] == "RETURN"
    ].iloc[0]

    assert sale["gross_revenue"] == 10
    assert returned["return_amount"] == 5
    assert returned["net_revenue"] == -5
    assert set(products["product_code"]) == {"X1"}
    assert len(rejects) == 1
    assert rejects.iloc[0]["reason"] == "unit_price_must_be_positive"


def test_product_code_is_normalized_and_missing_customer_is_kept():
    """Product codes are normalized and missing customers are allowed."""
    one = pd.DataFrame(
        [
            {
                **make_record(
                    "A1",
                    product=" ab 1 ",
                    customer="",
                    price="2",
                )
            }
        ]
    )

    two = pd.DataFrame(
        [
            {
                "Invoice": "A2",
                "StockCode": "CD2",
                "Description": "Other",
                "Quantity": "1",
                "InvoiceDate": "2010-12-01 09:00:00",
                "Price": "3",
                "Customer ID": "22",
                "Country": "UK",
            }
        ]
    )

    facts, _, _ = transform_sources(one, two)

    row = facts.loc[facts["invoice_no"] == "A1"].iloc[0]

    assert row["product_code"] == "AB1"
    assert pd.isna(row["customer_id"])


def test_duplicate_records_across_sources_are_deduplicated():
    """Equivalent transactions from both sources must appear once."""
    record = make_record(
        "INV-1",
        product="P-01",
        quantity="2",
        price="4.50",
    )

    one = pd.DataFrame([record])

    two = pd.DataFrame(
        [
            {
                "Invoice": "INV-1",
                "StockCode": "P-01",
                "Description": "SAMPLE ITEM",
                "Quantity": "2",
                "InvoiceDate": "2010-12-01 08:26",
                "Price": "4.50",
                "Customer ID": "100",
                "Country": "UK",
            }
        ]
    )

    facts, _, _ = transform_sources(one, two)

    assert len(facts) == 1


def test_invalid_quantity_is_logged_as_rejection():
    """A nonnumeric quantity is rejected without losing valid records."""
    one = pd.DataFrame(
        [
            make_record(
                "INV-2",
                product="P-02",
                quantity="not-a-number",
            )
        ]
    )

    two = pd.DataFrame(
        [
            {
                "Invoice": "INV-3",
                "StockCode": "P-03",
                "Description": "Sample",
                "Quantity": "1",
                "InvoiceDate": "2010-12-01 08:26",
                "Price": "4.50",
                "Customer ID": "100",
                "Country": "UK",
            }
        ]
    )

    facts, _, rejects = transform_sources(one, two)

    assert len(facts) == 1
    assert "invalid_quantity" in set(rejects["reason"])


def test_missing_invoice_is_rejected():
    """Transactions without an invoice number must be rejected."""
    one = pd.DataFrame(
        [make_record("", product="P01")]
    )

    facts, _, rejects = transform_sources(
        one,
        pd.DataFrame(columns=one.columns),
    )

    assert facts.empty
    assert "missing_invoice" in set(rejects["reason"])


def test_missing_product_code_is_rejected():
    """Transactions without a product code must be rejected."""
    one = pd.DataFrame(
        [make_record("INV-4", product="")]
    )

    facts, _, rejects = transform_sources(
        one,
        pd.DataFrame(columns=one.columns),
    )

    assert facts.empty
    assert "missing_product_code" in set(rejects["reason"])


def test_invalid_date_is_rejected():
    """Unparseable invoice dates must not enter the fact table."""
    one = pd.DataFrame(
        [make_record("INV-5", date="not-a-date")]
    )

    facts, _, rejects = transform_sources(
        one,
        pd.DataFrame(columns=one.columns),
    )

    assert facts.empty
    assert "invalid_invoice_date" in set(rejects["reason"])


def test_empty_inputs_return_empty_dataframes():
    """Empty sources must not cause transformation errors."""
    columns = [
        "InvoiceNo",
        "StockCode",
        "Description",
        "Quantity",
        "InvoiceDate",
        "UnitPrice",
        "CustomerID",
        "Country",
    ]

    empty_source = pd.DataFrame(columns=columns)

    facts, products, rejects = transform_sources(
        empty_source,
        empty_source,
    )

    assert facts.empty
    assert products.empty
    assert rejects.empty


def test_returns_do_not_generate_positive_sales_revenue():
    """A return must not be counted as a positive sale."""
    one = pd.DataFrame(
        [
            make_record(
                "INV-6",
                product="P06",
                quantity="-3",
                price="4",
            )
        ]
    )

    facts, _, rejects = transform_sources(
        one,
        pd.DataFrame(columns=one.columns),
    )

    assert rejects.empty
    assert len(facts) == 1
    assert facts.iloc[0]["gross_revenue"] == 0
    assert facts.iloc[0]["return_amount"] == 12
    assert facts.iloc[0]["net_revenue"] == -12