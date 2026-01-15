-- Create the staging database for ETL operations
CREATE DATABASE IF NOT EXISTS STAGING_DB;

-- Use the database
USE DATABASE STAGING_DB;

-- Create schema if needed
CREATE SCHEMA IF NOT EXISTS PUBLIC;
