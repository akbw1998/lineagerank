select
  cast(customer_id as bigint) as customer_id,
  first_name,
  last_name,
  lower(email) as email,
  cast(created_at as timestamp) as created_at
from {{ ref('customers') }}
