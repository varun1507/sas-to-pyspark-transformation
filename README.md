# SAS to PySpark Transformation

## 📌 Objective

Convert SAS-based ETL logic into PySpark and validate output consistency.

---

## 🧩 Business Use Case

This project simulates an e-commerce scenario where customer order data is processed to generate aggregated insights.

The goal is to transform raw transactional data into meaningful metrics for reporting and analytics.

---

## 🧠 Architecture

Source Layer → Transformation Layer (PySpark) → Aggregation → Validation Layer → Output Layer

---

## 🛠️ Tech Stack

* PySpark
* SAS
* Python

---

## 🔄 Pipeline Flow

1. Read source data (CSV)
2. Apply filtering logic
3. Perform aggregation
4. Validate results
5. Generate output
---
## 🔗 Join Logic  

Order data is joined with customer data using `customer_id` to enrich transactional data with customer attributes such as name and city.

This step enables generating meaningful business insights from raw data.

---

## ⚡ Key Highlights

* Recreated SAS logic using PySpark
* Applied filtering and aggregation
* Validated transformation results
* Designed scalable processing approach
* Implemented join between multiple datasets  

---

## 📂 Project Structure

* `data/` → Input dataset
* `sas/` → Original SAS logic
* `pyspark/` → PySpark transformation code
* `validation/` → Validation logic

---

## 🧪 Sample Transformation

### SAS Logic

```sas
proc sql;
  select customer_id, sum(amount) as total
  from orders
  where amount > 100
  group by customer_id;
quit;
```

### PySpark Equivalent

```python
df.filter("amount > 100") \
  .groupBy("customer_id") \
  .sum("amount")
```

---

## 📊 Sample Output  

customer_id | name  | city       | total_amount  
------------|-------|------------|--------------  
1           | Varun | Hyderabad  | 200  
2           | Rahul | Bangalore  | 300  

---

## 🚀 Impact

Demonstrates migration of legacy SAS ETL logic into scalable PySpark pipelines with validation and structured processing.
