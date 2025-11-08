/*
=============================================================
Create Database and Schemas
=============================================================
Script Purpose:
    This script creates a new database named 'QuickBite' . 
    Additionally, the script sets up three schemas 
    within the database: 'bronze', 'silver', and 'gold'.
	
*/

-- Create Database
create database QuickBite ;

use QuickBite ;

-- Create Schemas
create schema bronze ;
create schema silver ;
create schema gold ;