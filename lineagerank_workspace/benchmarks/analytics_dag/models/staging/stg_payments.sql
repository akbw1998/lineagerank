select
  cast(payment_id as bigint) as payment_id,
  cast(order_id as bigint) as order_id,
  payment_method,
  payment_status,
  cast(payment_amount as double) as payment_amount
from {{ ref('payments') }}
