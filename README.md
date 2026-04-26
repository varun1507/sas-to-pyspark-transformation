# SAS to PySpark Transformation

## 📌 Objective

Convert SAS-based ETL logic into PySpark and validate output consistency.

---

## 🧩 Business Use Case

This project simulates an e-commerce scenario where customer order data is processed to generate aggregated insights.

The goal is to transform raw transactional data into meaningful metrics for reporting and analytics.

---

## 🧠 Architecture  

Source Layer (CSV)  
→ Transformation Layer (PySpark)  
→ Aggregation Layer  
→ Join & Enrichment Layer  
→ Validation Layer  
→ Output Layer  

---

## 🛠️ Tech Stack

* PySpark (Data Processing)
* SAS (Legacy ETL)
* Python (Programming Language)
---
## 📥 Data Sources  

- `orders.csv` → Contains transactional order data  
- `customers.csv` → Contains customer details (name, city)  
---

## 🔄 Pipeline Flow  

1. Read orders and customer datasets from CSV files  
2. Apply business filtering logic (amount > 100)  
3. Aggregate total transaction amount per customer  
4. Join aggregated data with customer details  
5. Validate transformation results  
6. Generate enriched output dataset  
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
---
### PySpark Equivalent

```python
df.filter("amount > 100") \
  .groupBy("customer_id") \
  .sum("amount")
```

---

## 📊 Sample Output  

| customer_id | name  | city      | total_amount |
|------------|-------|-----------|--------------|
| 1          | Varun | Hyderabad | 200          |
| 2          | Rahul | Bangalore | 300          |

---
## ▶️ How to Run

1. Place input files (`orders.csv`, `customers.csv`) inside the `data/` folder

2. Run the PySpark script:

   ```bash
   python pyspark/transformation.py
   ```

3. The output will be displayed in the console

4. (Optional) Modify the script to write output to a file if needed

---

## 🚀 Impact

Demonstrates migration of legacy SAS ETL logic into scalable PySpark pipelines, including data enrichment, validation, and structured processing aligned with real-world data engineering practices.

---
