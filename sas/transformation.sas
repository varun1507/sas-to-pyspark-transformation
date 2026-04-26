proc sql;
  create table result as
  select customer_id, sum(amount) as total
  from orders
  where amount > 100
  group by customer_id;
quit;
