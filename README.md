# sumuptask
2025.01.15\
Sara Dias

## Overview
For this task, I created a pipeline using python (`sumup_task.py`) that:
1. reads the shared xlsx documents and transforms them into csv files,
2. writes the raw data to snowflake tables (located in `SUMUP_TAKEHOMETASK.RAW`)
3. using dbt, cleans the raw data in a staging environment (`models/staging`)
4. creates a dimensional model with dimension tables (`models/dimensions`), bridge tables (`models/bridges`) and fact tables (`models/facts`) 
5. builds a couple of datamarts to answer client specific questions/visualization tool ready tables

The script can be run locally - just replace the enviroment variables (username, password and account) by your own Snowflake credentials on the python file (l. 8-10) and on your local `.dbt\profiles.yml` (considering dbt is installed, otherwise installation is required to run the script).
If this is not possible, please reach out and I can execute it locally over a call. If possible, run `python sumup_task.py` in the present directory.
<br/>
<br/>
### Raw data to warehouse
The warehouse chosen for this project was Snowflake, considering it's the one used by the company. Snowflake doesn't support loading data to tables from xlsx files, so I converted them to csv files in the `convert_to_csv()` function.
I created tables for each of the raw datasets shared, specifying the schema (column names and field types).
To load the csv data to the Snowflake schemas, I first have to stage it, and only after load it into the respective table.

With this, 3 tables are created in the RAW dataset: `SUMUP_TAKEHOMETASK.RAW.DEVICE`, `SUMUP_TAKEHOMETASK.RAW.STORE`, `SUMUP_TAKEHOMETASK.RAW.TRANSACTION`. 
<br/>
<br/>
### Staging, data cleanup
In the step after, the raw data gets cleaned: 
- NULL values are  replaced by 'Unknown' and -99999, for string and numeric fields respectively;
- Special/misplaced characters (like commas in the  end of a string) are removed with regex_replace();
- Capitalization is applied equally to string fields;
- Considering possible GDPR compliance needs, card numbers and a addresses are hashed;
- Uniqueness and not_null dbt tests are applied to the id fields present in the 3 tables (`models/staging/schema.yml`);
- Not null filters applied to the string and timestamp fields of the 3 tables.

In order to organize the tables in distinct custom named datasets, I added the macro `generate_schema_name.sql` to overwrite Snowflake's default setting, that uses the default schema name in all custom schemas created after. With the macro, the schema named used in the `dbt_project.yml` file matches what can be found on the warehouse created tables.
The outcome of the data cleaning is stored as 3 tables in a staging dataset: `SUMUP_TAKEHOMETASK.STAGE.STG_DEVICE`, `SUMUP_TAKEHOMETASK.STAGE.STG_STORE`, `SUMUP_TAKEHOMETASK.STAGE.STG_TRANSACTION`. The scripts are in `models/staging`.
<br/>
<br/>
### Dimensional model
Once the data is cleaned, it's modelled following Kimball dimensional modeling principles, resembling a star schema. \
Each object, and its qualitative attributes - ex. a store id, its name, its address - are organized in dimension tables (`models/dimensions`), each with a unique identifier (primnary key). \
Each dimension table contains only attributed relative to the object they describe - ex. the dimension store won't contain information relative to any possible devices, as devices are entities in themselves.\
The goal is to normalise the data as much as reduce to avoid repetition - the more redundancies there are, the harder it is for stakeholders to navigate the warehouse and know from where to retrieve data.\
Besides the dimensions created, the model could have been more normalised/enriched with the creation of dimensions for countries, cities, status and dates.
The bridge tables(`models/bridges`) exist to establish the relationships between the dimensions unique keys, and how they can be used to buid the fact.\
The fact table (`models/facts`) is in the center of the dimensions; its unique identifier is the combination of the foreign keys of the dimension tables. Ideally, this table should only contain aggregations at the granularity it describes, but for usability purposes, some qualitative attributes, like store and device details, are added to simplify querying by stakeholders.\
In the fact table, considering the assumption that a transaction can be done to cover multiple products purchased, I split the transaction amount by all components of the primary key to make sure we don't end up accounting inflated transaction amounts. \
A simplified version of the schema can be seen bellow and in `dimensional_model.jpeg`:
<br/>
![dimensional_model](https://github.com/user-attachments/assets/c3f821c0-5701-4a20-a52a-90b44c6e57b5)
<br/>

The tables for the dimensional model were build using an incremental merge strategy. Considering heavy daily loads of data, this reduces the cost to reprocess historical data, as it looks only into newly arriving rows or rows that have been changed (`created_at`, `happened_at`) on the day the script is being executed. Considering lack of time/resources to orchestrate and schedule the pipeline for the case study, I setup a date (`2022-10-01`) as a variable to use as example.\
Similar to the staging layer, dbt tests were applied ton check for uniqueness of primary keys and non null values (on the `schema.yml` files, under each model directory).

Considering the assumption that transaction data is updated and re-exported - ex. a same transaction_id can have its status changed overtime -, in order to keep the historical records for all transactions recorded, I created a snapshot of dim_transactions (`snapshots/dim_transactions_history.sql`). This stores all historical values each row of the table has taken, with `dbt_valid_from` and `dbt_valid_to` as validity dates; the currently valid values, have `dbt_valid_to as NULL`.
<br/>
<br/>
### Datamarts and solution queries
On top of the dimensional model, for usability's sake, I built 2 datamarts(`models/datamarts`): `SUMUP_TAKEHOMETASK.DATAMARTS.STORE_PERFORMANCE` and `SUMUP_TAKEHOMETASK.DATAMARTS.DEVICE_PERFORMANCE`; these are aggregations tables that answer specific stakeholder questions about a certain topic.\
Their goal is to be easy to use on visualization tools and simple to ad hoc query.\
Using STORE_PERFORMANCE, we can answer: \
**a) Top 10 stores per transacted amount** \
`select store_id, sum(transaction_amount) amount ` \
`from SUMUP_TAKEHOMETASK.DATAMARTS.STORE_PERFORMANCE ` \
`group by store_id ` \
`order by amount desc ` \
`limit 10;`\
**b) Average transacted amount per store typology and country** \
`select store_typology, store_country, avg(transaction_amount) avg_amount`\
`from SUMUP_TAKEHOMETASK.DATAMARTS.STORE_PERFORMANCE` \
`group by store_typology, store_country;` \
**c) Average time for a store to perform its 5 first transactions**\
`select avg(minutes_to_fifth_accepted_transaction) avg_minutes_to_fifth_accepted_transaction` \
`from SUMUP_TAKEHOMETASK.DATAMARTS.STORE_PERFORMANCE` 

Using DEVICE_PERFORMANCE, we can answer: \
**a) Percentage of transactions per device type:** \
`select device_type, sum(ratio_transactions) perc_transactions ` \
`from SUMUP_TAKEHOMETASK.DATAMARTS.DEVICE_PERFORMANCE` \
`group by device_type;` 

And using the fact table directly we can answer: \
**a) Top 10 products sold:** \
`select product_name, count(transaction_id) transactions` \
`from SUMUP_TAKEHOMETASK.FACTS.FACT_STORE_DEVICE_TRANSACTIONS_DETAILS` \
`where transaction_last_status='accepted' ` \
`group by product_name` \
`order by transactions desc` \
`limit 10`

For the datamarts and the queries above, I made the assumption that, for a transaction to be a successful sale, its status needs to be 'accepted'.

Please reach out should you have any questions on the solution provided.
