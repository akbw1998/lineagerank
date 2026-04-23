
  
  create view "analytics_benchmark"."main"."stg_customers__dbt_tmp" as (
    select
  cast(customer_id as bigint) as customer_id,
  first_name,
  last_name,
  lower(email) as email,
  cast(created_at as timestamp) as created_at
from "analytics_benchmark"."main"."customers"
  );
