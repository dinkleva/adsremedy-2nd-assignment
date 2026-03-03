import pandas as pd
import random
import uuid
from faker import Faker
from datetime import datetime
import os

fake = Faker()
records = 1000
data = []

# Column Names: transaction_id, customer_id, amount, timestamp, merchant
for _ in range(records):
    data.append({
        "transaction_id": str(uuid.uuid4()),
        "customer_id": f"C{random.randint(10000, 99999)}",
        "amount": round(random.uniform(-50, 500), 2), # Includes negatives [cite: 45]
        "timestamp": fake.date_time_between(start_date='-30d', end_date='now').isoformat() + "Z",
        "merchant": f"STORE_{random.randint(1, 50)}"
    })

# Adding some explicit duplicates to test deduplication
for i in range(10):
    data.append(data[i])

output_path = "/opt/airflow/data/raw_transactions.json"
os.makedirs(os.path.dirname(output_path), exist_ok=True)

df = pd.DataFrame(data)
df.to_json(output_path, orient="records", lines=True)
print(f"Generated {len(data)} records in {output_path}")