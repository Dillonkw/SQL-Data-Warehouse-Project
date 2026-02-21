/*

================================================
Create Schemas
================================================

Script Purpose:
	This script sets up three schemas within the database: 'bronze', 'silver', and 'gold'.
*/

-- Dropping schema if it already exists
DROP SCHEMA IF EXISTS bronze CASCADE;
DROP SCHEMA IF EXISTS silver CASCADE;
DROP SCHEMA IF EXISTS gold CASCADE;

--Creating Schemas
CREATE SCHEMA bronze;
CREATE SCHEMA silver;
CREATE SCHEMA gold;
