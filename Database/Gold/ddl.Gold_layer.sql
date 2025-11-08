-- Create a master table with fact_orders,dim_customer,dim_restaurants,dim_delivery_partner


 ;
select * from silver.dim_delivery_partner ;

select * from silver.dim_customer ;
select * from silver.dim_restaurant ;
select * from silver.fact_ratings ;
select * from silver.fact_order_items ;
select * from silver.fact_delivery_performance;
select * from silver.fact_orders ;

-- -- Create a master table with fact_orders,dim_customer,dim_restaurants,dim_delivery_partner
create view  gold.fact_orders as 
select 
	f.order_id,
	c.customer_id,
	r.restaurant_id,
	f.order_timestamp,
	f.order_period,
	f.subtotal_amount,
	f.discount_amount,
	f.delivery_fee,
	f.total_amount,
	f.is_cod,
	f.is_cancelled,
	c.city as customer_city,
	r.restaurant_name,
	r.city as restaurant_city,
	r.partner_type,
	r.avg_prep_time_min,
	r.is_active as restaurant_active,
	d.delivery_partner_id,
	d.vehicle_type,
	d.employment_type,
	d.avg_rating,
	d.is_active as del_partner_active
from silver.fact_orders f
left join silver.dim_customer c
on   f.customer_id = c.customer_id
left join silver.dim_restaurant r
on   f.restaurant_id = r.restaurant_id
left join silver.dim_delivery_partner d
on   f.delivery_partner_id = d.delivery_partner_id

-- High Value Customers
select 
customer_id,
count(customer_id) as total_orders
from gold.fact_master
group by customer_id
having count(customer_id) > 3


-- Create a view for joining tables: orders_rating by joing silver.fact_orders and silver.fact_ratings(inner-join)
create view gold.fact_customer_ratings as
select 
	c.customer_id,
	c.signup_date,
	c.city,
	c.cust_period,
	c.acquisition_channel,
	r.rating,
	r.rating_category,
	r.review_text,
	r.review_timestamp,
	r.sentiment_score,
	r.sentiment_category
from  silver.dim_customer c
inner join silver.fact_ratings r
on   c.customer_id = r.customer_id


-- Create a view for joining tables: restaurant_ratings by joing silver.fact_ratings and silver.dim_restaurant(inner-join)
create view gold.fact_restaurant_ratings as
select 
	d.restaurant_id,
	d.restaurant_name,
	d.city,
	d.partner_type,
	d.avg_prep_time_min,
	d.is_active as restaurant_active,
	d.is_active,
	f.rating,
	f.rating_category,
	f.review_text,
	f.review_timestamp ,
	f.sentiment_score,
	f.sentiment_category
from  silver.dim_restaurant d
inner join silver.fact_ratings f
on   d.restaurant_id = f.restaurant_id


-- Create a view for joining tables: fact_orders,facT_delivery_performance,dim_customers,dim_restaurant
create view gold.fact_CRD_performance as
select 
    o.order_id,
    o.order_timestamp,
	o.order_period,
	o.subtotal_amount,
	o.discount_amount,
	o.delivery_fee,
    o.total_amount,
	o.is_cod,
	o.is_cancelled,
    c.customer_id,
	c.signup_date,
    c.city,
    c.cust_period,
    c.acquisition_channel,
    r.restaurant_id,
    r.restaurant_name,
	r.partner_type,
	r.avg_prep_time_min,
	r.is_active,
    d.delivery_status,
    d.distance_km
from silver.fact_orders o
inner join silver.fact_delivery_performance d
    on o.order_id = d.order_id
inner join silver.dim_customer c
    on o.customer_id = c.customer_id
inner join silver.dim_restaurant r
    on o.restaurant_id = r.restaurant_id;



