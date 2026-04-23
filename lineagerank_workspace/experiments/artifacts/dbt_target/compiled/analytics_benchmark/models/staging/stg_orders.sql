select
  cast(order_id as bigint) as order_id,
  cast(customer_id as bigint) as customer_id,
  order_status,
  cast(order_date as date) as order_date,
  cast(amount as double) as amount
from "analytics_benchmark"."main"."orders"