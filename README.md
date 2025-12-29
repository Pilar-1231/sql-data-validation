# SQL Data Validation

This repository contains a SQL query that demonstrates data validation logic using business rules and tolerances.

The purpose of this project is to show how SQL can be used to validate structured data, detect inconsistencies, and ensure data quality across multiple sources.

## What the query does
- Reads structured input data (JSON fields) and converts them into typed columns
- Applies business rules to validate required fields
- Detects NULL values and duplicated records
- Validates date ranges and numeric constraints
- Recalculates expected values using tolerances
- Cross-checks records against reference tables
- Generates validation flags (OK / NO OK)

## Key SQL concepts used
- Common Table Expressions (CTEs)
- JOINs across multiple sources
- CASE WHEN logic
- Data type casting and normalization
- Numeric tolerance comparisons
- Date validation rules

## Notes
- This is a simulated and anonymized example created for portfolio purposes
- Schema, table, and column names were modified to protect sensitive information
- No real or confidential data is included

## Use case
This type of SQL logic is commonly used in data validation, data quality checks, and reconciliation processes before reporting or downstream consumption.
