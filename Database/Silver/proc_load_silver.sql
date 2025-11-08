/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME; 
    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT '================================================';
        PRINT 'Loading Silver Layer';
        PRINT '================================================';

		PRINT '------------------------------------------------';
		PRINT 'Loading Fact Tables';
		PRINT '------------------------------------------------';

		-- Loading silver.fact_order_items
        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.fact_order_items';
		TRUNCATE TABLE silver.fact_order_items;
		PRINT '>> Inserting Data Into: silver.fact_order_items';
		INSERT INTO silver.fact_order_items (
			order_id,
			menu_item_id,
			restaurant_id,
			quantity,
			unit_price,
			item_discount,
			line_total
		)
		SELECT
			order_id,
			menu_item_id,
			restaurant_id,
			quantity,
			unit_price,
			item_discount,
			line_total
		FROM bronze.fact_order_items ;
		SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

		-- Loading silver.fact_orders
        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.fact_orders';
		TRUNCATE TABLE silver.fact_orders;
		PRINT '>> Inserting Data Into: silver.fact_orders';
		INSERT INTO silver.fact_orders (
			order_id,
			customer_id,
			restaurant_id,
			delivery_partner_id,
			order_timestamp,
			order_period,
			subtotal_amount,
			discount_amount,
			delivery_fee,
			total_amount,
			is_cod,
			is_cancelled
		)
		SELECT
			order_id,
			customer_id,
			restaurant_id,
			delivery_partner_id,
			order_timestamp,
			CASE WHEN order_timestamp >= '2025-01-01' and order_timestamp <= '2025-05-31' then 'Pre-crisis'
				 WHEN order_timestamp > '2025-05-31' then 'Post-crisis'
				 ELSE 'n/a'
			END  AS order_period,
			subtotal_amount,
			discount_amount,
			delivery_fee,
			total_amount,
			is_cod,
			is_cancelled
		FROM bronze.fact_orders ;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        -- Loading silver.fact_ratings
        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.fact_ratings';
		TRUNCATE TABLE silver.fact_ratings;
		PRINT '>> Inserting Data Into: silver.fact_ratings';
		INSERT INTO silver.fact_ratings (
					order_id,
					customer_id,
					restaurant_id,
					rating,
					rating_category,
					review_text,
					review_timestamp,
					sentiment_score,
					sentiment_category
				)
		SELECT
			order_id,
			customer_id,
			restaurant_id,
			rating,
			CASE 
				WHEN rating >= 1.0 AND rating < 3.0 THEN 'Worse'
				WHEN rating >= 3.0 AND rating < 4.0 THEN 'Average'
				WHEN rating >= 4.0 AND rating <= 5.0 THEN 'Excellent'
				ELSE 'n/a'
			END,
			review_text,
			TRY_CONVERT(datetime2,
				SUBSTRING(review_timestamp, 7, 4) + '-' +   -- YYYY
				SUBSTRING(review_timestamp, 4, 2) + '-' +   -- MM
				SUBSTRING(review_timestamp, 1, 2) + ' ' +   -- DD
				SUBSTRING(review_timestamp, 12, 5) + ':00'  -- HH:MM + seconds
			) AS review_timestamp,
			sentiment_score,
			CASE 
				WHEN sentiment_score < 0 THEN 'Negative'
				WHEN sentiment_score = 0 THEN 'Neutral'
				WHEN sentiment_score > 0 THEN 'Positive'
				ELSE 'n/a'
			END
		FROM bronze.fact_ratings
		WHERE order_id IS NOT NULL;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        -- Loading silver.fact_delivery_performance
        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.fact_delivery_performance';
		TRUNCATE TABLE silver.fact_delivery_performance;
		PRINT '>> Inserting Data Into: silver.fact_delivery_performance';
		INSERT INTO silver.fact_delivery_performance (
			order_id,
			actual_delivery_time_mins,
			expected_delivery_time_mins,
			distance_km,
			delivery_status 
			)
		SELECT
			order_id,
			actual_delivery_time_mins,
			expected_delivery_time_mins,
			distance_km,
			CASE 
			WHEN actual_delivery_time_mins - expected_delivery_time_mins > 0 THEN 'Late'
			WHEN actual_delivery_time_mins - expected_delivery_time_mins < 0 THEN 'Early'
			ELSE 'On-Time'
			END AS delivery_status
		FROM bronze.fact_delivery_performance ;
		SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

		PRINT '------------------------------------------------';
		PRINT 'Loading Dimension Tables';
		PRINT '------------------------------------------------';

        -- Loading silver.dim_customer
        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.dim_customer';
		TRUNCATE TABLE silver.dim_customer;
		PRINT '>> Inserting Data Into: silver.dim_customer';
		INSERT INTO silver.dim_customer (
			customer_id,
			signup_date,
			city,
			cust_period,
			acquisition_channel
		)
		SELECT
			customer_id,
			signup_date,
			city,
			CASE WHEN signup_date >= '2025-01-01' and signup_date <= '2025-05-31' then 'Pre-crisis'
			WHEN signup_date > '2025-05-31' then 'Post-crisis'
			ELSE 'n/a'
			END  'cust_period',
			acquisition_channel
		FROM bronze.dim_customer;

	    SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';
		
		-- Loading silver.dim_restaurant
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.dim_restaurant';
		TRUNCATE TABLE silver.dim_restaurant;
		PRINT '>> Inserting Data Into: silver.dim_restaurant';
		INSERT INTO silver.dim_restaurant (
			restaurant_id,
			restaurant_name,
			city,
			cuisine_type,
			partner_type,
			avg_prep_time_min,
			is_active
		)
		SELECT
			restaurant_id,
			restaurant_name,
			city,
			cuisine_type,
			partner_type,
			avg_prep_time_min,
			is_active
		FROM bronze.dim_restaurant;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

		-- Loading silver.dim_delivery_partner
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.dim_delivery_partner';
		TRUNCATE TABLE silver.dim_delivery_partner;
		PRINT '>> Inserting Data Into: silver.dim_delivery_partner';
		INSERT INTO silver.dim_delivery_partner (
			delivery_partner_id,
			partner_name,
			city,
			vehicle_type,
			employment_type,
			avg_rating,
			is_active
		)
		SELECT
			delivery_partner_id,
			partner_name,
			city,
			vehicle_type,
			employment_type,
			avg_rating,
			is_active
		FROM bronze.dim_delivery_partner;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

		-- Loading silver.dim_menu_item
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.dim_menu_item';
		TRUNCATE TABLE silver.dim_menu_item;
		PRINT '>> Inserting Data Into: silver.dim_menu_item';
		INSERT INTO silver.dim_menu_item (
			menu_item_id,
			restaurant_id,
			item_name,
			category,
			is_veg,
			price
		)
		SELECT
			menu_item_id,
			restaurant_id,
			item_name,
			category,
			is_veg,
			price
		FROM bronze.dim_menu_item ;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

		SET @batch_end_time = GETDATE();
		PRINT '=========================================='
		PRINT 'Loading Silver Layer is Completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '=========================================='
		
	END TRY
	BEGIN CATCH
		PRINT '=========================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================='
	END CATCH
END
