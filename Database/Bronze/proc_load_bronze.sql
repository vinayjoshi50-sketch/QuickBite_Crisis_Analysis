/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME; 
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '================================================';
		PRINT 'Loading Bronze Layer';
		PRINT '================================================';

		PRINT '------------------------------------------------';
		PRINT 'Loading Fact Tables';
		PRINT '------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.fact_orders';
		TRUNCATE TABLE bronze.fact_orders;
		PRINT '>> Inserting Data Into: bronze.fact_orders';
		BULK INSERT bronze.fact_orders
		FROM 'C:\Users\vjosh\Downloads\QuickBite\Datasets\fact_orders.csv'
		WITH (
			FIRSTROW = 2,                 
			FIELDTERMINATOR = ',',        
			ROWTERMINATOR = '0x0a',     
			TABLOCK
);

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.fact_order_items';
		TRUNCATE TABLE bronze.fact_order_items;
		PRINT '>> Inserting Data Into: bronze.fact_order_items';
		BULK INSERT bronze.fact_order_items
		FROM 'C:\Users\vjosh\Downloads\rpc_18_inputs_for_participants\Datasets\fact_order_items.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '0x0a',
			TABLOCK
		);	
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';


        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.fact_ratings';
		TRUNCATE TABLE bronze.fact_ratings;
		PRINT '>> Inserting Data Into: bronze.fact_ratings';
		BULK INSERT bronze.fact_ratings
		FROM 'C:\Users\vjosh\Downloads\rpc_18_inputs_for_participants\Datasets\fact_ratings.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '0x0a',
			TABLOCK
		);	
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

				
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.fact_delivery_performance';
		TRUNCATE TABLE bronze.fact_delivery_performance;
		PRINT '>> Inserting Data Into: bronze.fact_delivery_performance';
		BULK INSERT bronze.fact_delivery_performance
		FROM 'C:\Users\vjosh\Downloads\rpc_18_inputs_for_participants\Datasets\fact_delivery_performance.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '0x0a',
			TABLOCK
		);	

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		PRINT '------------------------------------------------';
		PRINT 'Loading Dimension Tables';
		PRINT '------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.dim_customer';
		TRUNCATE TABLE bronze.dim_customer;
		PRINT '>> Inserting Data Into: bronze.dim_customer';
		
		BULK INSERT bronze.dim_customer_staging
		FROM 'C:\Users\vjosh\Downloads\QuickBite\Datasets\dim_customer.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '\n',
			TABLOCK
		);

		INSERT INTO bronze.dim_customer (customer_id, signup_date, city, acquisition_channel)
		SELECT
			customer_id,
			TRY_CONVERT(date, signup_date, 105),  -- handles dd-MM-yyyy
			city,
			acquisition_channel
		FROM bronze.dim_customer_staging;


		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.dim_restaurant';
		TRUNCATE TABLE bronze.dim_restaurant;
		PRINT '>> Inserting Data Into: bronze.dim_restaurant';
		BULK INSERT bronze.dim_restaurant
		FROM 'C:\Users\vjosh\Downloads\QuickBite\Datasets\dim_restaurant.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.dim_delivery_partner';
		TRUNCATE TABLE bronze.dim_delivery_partner;
		PRINT '>> Inserting Data Into: bronze.dim_delivery_partner';
		BULK INSERT bronze.dim_delivery_partner
		FROM 'C:\Users\vjosh\Downloads\rpc_18_inputs_for_participants\Datasets\dim_delivery_partner.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '0x0a',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.dim_menu_item';
		TRUNCATE TABLE bronze.dim_menu_item;
		PRINT '>> Inserting Data Into: bronze.dim_menu_item';
		BULK INSERT bronze.dim_menu_item
		FROM 'C:\Users\vjosh\Downloads\rpc_18_inputs_for_participants\Datasets\dim_menu_item.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '0x0a',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';


		SET @batch_end_time = GETDATE();
		PRINT '=========================================='
		PRINT 'Loading Bronze Layer is Completed';
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
