# SAS to PySpark Transformation

## 📌 Objective

Convert SAS-based ETL logic into PySpark and validate output consistency.
-----
## 🧩 Business Use Case  

This project simulates an e-commerce scenario where customer order data is processed to generate aggregated insights.

The goal is to transform raw transactional data into meaningful metrics for reporting and analytics.

---

## 🧠 Architecture

Source Data → PySpark Transformation → Aggregation → Validation → Output

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

## ⚡ Key Highlights

* Recreated SAS logic using PySpark
* Applied filtering and aggregation
* Validated transformation results
* Designed scalable processing approach

---

## 📂 Project Structure

data/ → input dataset
sas/ → original SAS logic
pyspark/ → PySpark transformation code
validation/ → result validation logic

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

## 📊 Outcome

* Achieved equivalent results between SAS and PySpark
* Demonstrated scalable data processing using Spark
