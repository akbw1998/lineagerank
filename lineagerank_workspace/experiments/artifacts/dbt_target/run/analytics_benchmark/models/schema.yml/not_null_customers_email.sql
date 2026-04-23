
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select email
from "analytics_benchmark"."main"."customers"
where email is null



  
  
      
    ) dbt_internal_test