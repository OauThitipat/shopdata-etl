# ============================================================
# Unit tests สำหรับฟังก์ชันล้างข้อมูลใน pipeline.py
#
# - ใช้ข้อมูลปลอม (fake DataFrame) ไม่ต่อ database จริง
# - วิธีรัน (How to run):  python -m pytest -v
# ============================================================

import pandas as pd
import pytest

from pipeline import (
    DEFAULT_EMAIL,
    clean_customers,
    clean_orders,
    convert_to_usd,
    deduplicate_customers,
    fill_missing_emails,
    filter_invalid_orders,
    standardize_phone,
)


# ------------------------------------------------------------
# ข้อมูลปลอมที่ใช้ร่วมกัน (shared fake data)
# ------------------------------------------------------------
@pytest.fixture
def rates():
    # อัตราแลกเปลี่ยนปลอม (fake exchange rates)
    return pd.DataFrame({
        "currency": ["EUR", "EUR", "JPY"],
        "rate_to_usd": [1.10, 1.20, 0.007],
        "date": ["2023-05-01", "2023-05-02", "2023-05-01"],
    })


def make_orders(rows):
    # ตัวช่วยสร้างตารางออเดอร์ปลอม (helper to build fake orders)
    columns = ["order_id", "customer_id", "order_date", "total_amount", "currency", "status"]
    return pd.DataFrame(rows, columns=columns)


# ------------------------------------------------------------
# 1. เบอร์โทร (phone number)
# ------------------------------------------------------------
# parametrize = รัน test เดียวกันหลายกรณี (one test, many inputs)
@pytest.mark.parametrize("raw, expected", [
    ("+1 (555) 123-4567", "15551234567"),   # ตัวอย่างจากโจทย์
    ("555-987-6543", "5559876543"),
    ("(555) 333 4444", "5553334444"),
    ("+44 20 7123 1234", "442071231234"),   # เบอร์ต่างประเทศ
    ("15551234567", "15551234567"),         # สะอาดอยู่แล้ว
    (5551234567, "5551234567"),             # input เป็นตัวเลข
])
def test_standardize_phone_strips_non_numeric(raw, expected):
    assert standardize_phone(raw) == expected


# ไม่มีเบอร์ หรือไม่มีตัวเลขเลย -> ต้องได้ None
@pytest.mark.parametrize("raw", [None, float("nan"), pd.NA, "", "N/A"])
def test_standardize_phone_returns_none_when_no_digits(raw):
    assert standardize_phone(raw) is None


# ------------------------------------------------------------
# 2. ลูกค้า (customers)
# ------------------------------------------------------------
def test_deduplicate_keeps_most_recent_signup():
    # Alice มี 2 แถว -> ต้องเหลือแถวที่สมัครล่าสุด
    df = pd.DataFrame({
        "customer_id": [1, 1, 2],
        "full_name": ["Alice", "Alice", "Bob"],
        "email": ["old@x.com", "new@x.com", "bob@x.com"],
        "phone": [None, None, None],
        "signup_date": ["2023-01-15", "2023-06-01", "2023-02-20"],
    })

    result = deduplicate_customers(df)

    assert result["customer_id"].tolist() == [1, 2]
    alice = result[result["customer_id"] == 1]
    assert alice["email"].iloc[0] == "new@x.com"


def test_fill_missing_emails_replaces_null_and_blank():
    # None, เว้นวรรค, NaN -> ต้องกลายเป็น unknown@domain.com
    df = pd.DataFrame({"email": ["a@x.com", None, "  ", float("nan")]})

    result = fill_missing_emails(df)

    assert result["email"].tolist() == ["a@x.com", DEFAULT_EMAIL, DEFAULT_EMAIL, DEFAULT_EMAIL]


def test_clean_customers_end_to_end():
    # Bob 2 แถว ไม่มีอีเมลทั้งคู่ -> เหลือ 1 แถว + อีเมล default + เบอร์เป็นตัวเลข
    df = pd.DataFrame({
        "customer_id": [2, 2],
        "full_name": ["Bob Jones", "Bob Jones"],
        "email": [None, None],
        "phone": ["555-987-6543", "(555) 987-6543"],
        "signup_date": ["2023-02-20", "2023-09-15"],
    })

    result = clean_customers(df)

    assert len(result) == 1
    row = result.iloc[0]
    assert row["email"] == DEFAULT_EMAIL
    assert row["phone"] == "5559876543"
    assert row["signup_date"] == "2023-09-15"


# ------------------------------------------------------------
# 3. ออเดอร์ (orders)
# ------------------------------------------------------------
def test_filter_invalid_orders_drops_non_positive_and_null_amounts():
    # ยอด 0, ติดลบ, ว่าง -> ถูกตัด / 0.01 ยังต้องอยู่ (ค่าขอบ edge case)
    orders = make_orders([
        (1, 1, "2023-05-01", 100.0, "USD", "COMPLETED"),
        (2, 1, "2023-05-01", 0.0, "USD", "COMPLETED"),
        (3, 1, "2023-05-01", -50.0, "USD", "SYSTEM_ERROR"),
        (4, 1, "2023-05-01", None, "USD", "COMPLETED"),
        (5, 1, "2023-05-01", 0.01, "USD", "COMPLETED"),
    ])

    result = filter_invalid_orders(orders)

    assert result["order_id"].tolist() == [1, 5]


def test_convert_to_usd_uses_rate_for_order_date(rates):
    # วันต่างกัน -> ใช้อัตราต่างกัน (different day, different rate)
    orders = make_orders([
        (1, 1, "2023-05-01", 100.0, "EUR", "COMPLETED"),    # 100 x 1.10
        (2, 1, "2023-05-02", 100.0, "EUR", "COMPLETED"),    # 100 x 1.20
        (3, 1, "2023-05-01", 10000.0, "JPY", "COMPLETED"),  # 10000 x 0.007
    ])

    result = convert_to_usd(orders, rates)

    # pytest.approx = เทียบทศนิยมแบบยอมคลาดเคลื่อนนิดหน่อย
    assert result["usd_amount"].tolist() == pytest.approx([110.0, 120.0, 70.0])


def test_convert_to_usd_treats_missing_currency_as_usd(rates):
    # ไม่มีสกุลเงิน -> USD
    orders = make_orders([(1, 1, "2023-05-01", 120.0, None, "COMPLETED")])

    result = convert_to_usd(orders, rates)

    assert result["currency"].iloc[0] == "USD"
    assert result["usd_amount"].iloc[0] == pytest.approx(120.0)


def test_convert_to_usd_treats_missing_rate_as_usd(rates):
    # EUR วันที่ไม่มีอัตรา + GBP ที่ไม่มีอัตราเลย -> rate = 1.0
    orders = make_orders([
        (1, 1, "2023-05-09", 50.0, "EUR", "COMPLETED"),
        (2, 1, "2023-05-01", 80.0, "GBP", "COMPLETED"),
    ])

    result = convert_to_usd(orders, rates)

    assert result["rate_to_usd"].tolist() == [1.0, 1.0]
    assert result["usd_amount"].tolist() == [50.0, 80.0]


def test_convert_to_usd_normalises_currency_codes(rates):
    # " eur " (ตัวเล็ก มีช่องว่าง) -> ต้องใช้อัตรา EUR ได้
    orders = make_orders([(1, 1, "2023-05-01", 100.0, " eur ", "COMPLETED")])

    result = convert_to_usd(orders, rates)

    assert result["usd_amount"].iloc[0] == pytest.approx(110.0)


def test_convert_to_usd_does_not_duplicate_rows_on_duplicate_rates():
    # อัตราซ้ำ 2 แถว -> join แล้วออเดอร์ต้องยังเป็น 1 แถว
    duplicate_rates = pd.DataFrame({
        "currency": ["EUR", "EUR"],
        "rate_to_usd": [1.1, 1.1],
        "date": ["2023-05-01", "2023-05-01"],
    })
    orders = make_orders([(1, 1, "2023-05-01", 100.0, "EUR", "COMPLETED")])

    result = convert_to_usd(orders, duplicate_rates)

    assert len(result) == 1


def test_clean_orders_filters_then_converts(rates):
    # ออเดอร์ติดลบถูกตัด ที่เหลือมี usd_amount
    orders = make_orders([
        (1, 1, "2023-05-01", 100.0, "EUR", "COMPLETED"),
        (2, 1, "2023-05-01", -100.0, "EUR", "SYSTEM_ERROR"),
    ])

    result = clean_orders(orders, rates)

    assert result["order_id"].tolist() == [1]
    assert "usd_amount" in result.columns
