
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select gross_revenue
from "analytics_benchmark"."main"."customer_revenue"
where gross_revenue is null



  
  
      
    ) dbt_internal_test