-- Data Cleaning

-- Check for null or duplicates
-- Expectation: No result
select
order_id,
count(*)
from [bronze].[fact_orders]
group by order_id
having count(*) > 1 or order_id is null

-- Check for invalid dates
select
order_timestamp
from [bronze].[fact_orders]
where order_timestamp is null ;

-- Check for unwanted spaces 
select
*
from bronze.dim_delivery_partner
where is_active != trim(is_active) ;

select
*
from bronze.dim_customer
where acquisition_channel != trim(acquisition_channel) ;

-- Check for invalid values 
select
*
from [bronze].[fact_orders]
where subtotal_amount < 0 ;

-- Check for Formula
select
*
from [bronze].[fact_order_items]
where line_total != quantity * unit_price - (item_discount)

-- Check the distance values
select
*
from bronze.fact_delivery_performance
where distance_km <= 0 ;

