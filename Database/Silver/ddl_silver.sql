/*
===============================================================================
DDL Script: Create silver Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'silver' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'silver' Tables
===============================================================================
*/


If OBJECT_ID('silver.fact_orders', 'U') IS NOT NULL
    DROP TABLE silver.fact_orders;
Go

Create Table silver.fact_orders (
    order_id nvarchar(50),
    customer_id  nvarchar(50),
    restaurant_id  nvarchar(50),
    delivery_partner_id  nvarchar(50),
    order_timestamp  datetime2,
    order_period       nvarchar(50),
    subtotal_amount   decimal(10,2),
    discount_amount    decimal(10,2),
    delivery_fee       decimal(10,2),
    total_amount      decimal(10,2),
    is_cod          nvarchar(10),
    is_cancelled     nvarchar(10)
    ) ;
Go

IF OBJECT_ID('silver.fact_order_items', 'U') IS NOT NULL
    DROP TABLE silver.fact_order_items;
Create Table silver.fact_order_items(
    order_id        nvarchar(50),
    menu_item_id    nvarchar(50),
    restaurant_id   nvarchar(50),
    quantity        int,
    unit_price      decimal(10,2),
    item_discount   decimal(10,2), 
    line_total      decimal(10,2),
    );
Go

IF OBJECT_ID('silver.fact_ratings', 'U') IS NOT NULL
    DROP TABLE silver.fact_ratings;
Create Table silver.fact_ratings(
order_id          nvarchar(50),
customer_id       nvarchar(50),
restaurant_id     nvarchar(50),
rating            decimal(10,1),
rating_category    nvarchar(50),
review_text       nvarchar(100), 
review_timestamp    datetime2,
sentiment_score     decimal(10,2),
sentiment_category   nvarchar(50)
    );
Go

IF OBJECT_ID('silver.fact_delivery_performance', 'U') IS NOT NULL
    DROP TABLE silver.fact_delivery_performance;
Create Table silver.fact_delivery_performance(
order_id                          nvarchar(50),
actual_delivery_time_mins         int,
expected_delivery_time_mins       int,
distance_km                       decimal(5,1),
delivery_status                   nvarchar(50)
    );
Go

IF OBJECT_ID('silver.dim_customer', 'U') IS NOT NULL
    DROP TABLE silver.dim_customer;
Create Table silver.dim_customer(
customer_id   nvarchar(50),
signup_date     nvarchar(50),
city           nvarchar(50),
cust_period     nvarchar(50),
acquisition_channel   nvarchar(50)
    );
Go

IF OBJECT_ID('silver.dim_restaurant', 'U') IS NOT NULL
    DROP TABLE silver.dim_restaurant;
Create Table silver.dim_restaurant(
restaurant_id      nvarchar(50),
restaurant_name   nvarchar(100),
city               nvarchar(50),
cuisine_type        nvarchar(50),
partner_type         nvarchar(50),
avg_prep_time_min    nvarchar(50),
is_active             nvarchar(50)
    );
Go

IF OBJECT_ID('silver.dim_delivery_partner', 'U') IS NOT NULL
    DROP TABLE silver.dim_delivery_partner;
Create Table silver.dim_delivery_partner(
delivery_partner_id     nvarchar(50),
partner_name            nvarchar(50),
city                    nvarchar(50),
vehicle_type            nvarchar(50),
employment_type         nvarchar(50),
avg_rating             decimal(10,2),
is_active              nvarchar(50)
    );
Go

IF OBJECT_ID('silver.dim_menu_item', 'U') IS NOT NULL
    DROP TABLE silver.dim_menu_item;
Create Table silver.dim_menu_item(
menu_item_id      nvarchar(50),
restaurant_id     nvarchar(50),
item_name         nvarchar(100),
category          nvarchar(50),
is_veg            nvarchar(50),
price             decimal(10,2)
    );
Go








