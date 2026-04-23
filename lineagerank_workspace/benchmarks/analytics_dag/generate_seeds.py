from __future__ import annotations

import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

from faker import Faker


def write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    random.seed(42)
    fake = Faker()
    Faker.seed(42)

    root = Path(__file__).resolve().parent
    seeds_dir = root / "seeds"
    seeds_dir.mkdir(parents=True, exist_ok=True)

    customers: list[dict[str, object]] = []
    for customer_id in range(1, 1001):
        customers.append(
            {
                "customer_id": customer_id,
                "first_name": fake.first_name(),
                "last_name": fake.last_name(),
                "email": f"customer_{customer_id}@example.com",
                "created_at": (datetime(2024, 1, 1) + timedelta(days=random.randint(0, 365))).isoformat(),
            }
        )

    orders: list[dict[str, object]] = []
    for order_id in range(1, 10001):
        order_date = datetime(2024, 1, 1) + timedelta(days=random.randint(0, 365))
        orders.append(
            {
                "order_id": order_id,
                "customer_id": random.randint(1, 1000),
                "order_status": random.choice(["placed", "shipped", "completed", "returned"]),
                "order_date": order_date.date().isoformat(),
                "amount": round(random.uniform(15.0, 650.0), 2),
            }
        )

    payment_methods = ["credit_card", "debit_card", "paypal", "gift_card"]
    payments: list[dict[str, object]] = []
    for payment_id in range(1, 10001):
        payments.append(
            {
                "payment_id": payment_id,
                "order_id": payment_id,
                "payment_method": random.choice(payment_methods),
                "payment_status": random.choice(["paid", "paid", "paid", "pending"]),
                "payment_amount": round(random.uniform(15.0, 650.0), 2),
            }
        )

    write_csv(
        seeds_dir / "customers.csv",
        ["customer_id", "first_name", "last_name", "email", "created_at"],
        customers,
    )
    write_csv(
        seeds_dir / "orders.csv",
        ["order_id", "customer_id", "order_status", "order_date", "amount"],
        orders,
    )
    write_csv(
        seeds_dir / "payments.csv",
        ["payment_id", "order_id", "payment_method", "payment_status", "payment_amount"],
        payments,
    )


if __name__ == "__main__":
    main()
