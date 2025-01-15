# sumuptask
2025.01.15\
Sara Dias

### Overview
For this task, I created a pipeline using python (`sumup_task.py`) that:
1. reads the shared xlsx documents and transforms them into csv files,
2. writes the raw data to snowflake tables (located in `SUMUP_TAKEHOMETASK.RAW`)
3. using dbt, cleans the raw data in a staging environment (`models/staging`)
4. creates a dimensional model with dimension tables (`models/dimensions`), bridge tables (`models/bridges`) and fact tables (`models/facts`) 
5. builds a couple of datamarts to answer client specific questions/visualization tool ready tables

The script can be run locally - just replace the enviroment variables (username, password and account) by your own Snowflake credentials on the python file (l.8-10) and on your local .dbt\profiles.yml (considering dbt is installed, otherwise installation is required to run the script).
If this is not possible, please reach out and I can execute it locally over a call.
<br/>
<br/>
#### Raw data to warehouse
The warehouse chosen for this project was Snowflake, considering its the one used by the company. Snowflake doesnt support loading data to tables from xlsx files, so I converted them to csv files in the convert_to_csv() function.
I created tables for each of the raw datasets shared, specifying the schema and field types.
To load the csv data to the Snowflake schemas, I first have to stage it, and only after load it into the respective table.

With this, 3 tables are created in the RAW dataset: `SUMUP_TAKEHOMETASK.RAW.DEVICE`, `SUMUP_TAKEHOMETASK.RAW.STORE`, `SUMUP_TAKEHOMETASK.RAW.TRANSACTION`. 
<br/>
<br/>
#### Staging, data cleanup
In the step after, the raw data gets cleaned: 
- NULL values are  replaced by 'Unknown' and -99999, for string and numeric fields respectively;
- Special/misplaced characters (like commas in the  end of a string) are removed with regex_replace();
- Capitalization is applied equally to string fields;
- Considering possible GDPR compliance needs, card numbers and a addresses are hashed;
- Uniqueness and not_null dbt tests are applied to the id fields present in the 3 tables (`models/staging/schema.yml`);
- Not null filters applied to the string and timestamp fields of the 3 tables.

In lorder to organize the tables in distinctly custom named datasets, I added the macro generate_schema_name.sql to overwrite a Snowflake default setting, that uses the default schema name in all custome schemas created after. This way, the schema named used in the dwb_project.yml file matches what can be found on the warehouse created tables.
The outcome of the data cleaning is stored as 3 tables in a staging dataset: `SUMUP_TAKEHOMETASK.STAGE.STG_DEVICE`, `SUMUP_TAKEHOMETASK.STAGE.STG_STORE`, `SUMUP_TAKEHOMETASK.STAGE.STG_TRANSACTION`. The scripts are in `models/staging`.
<br/>
<br/>
#### Dimensional model
Once the data is cleaned, its modelled following Kimball dimensional modeling principles. \
Each object, and its qualitative attributes - ex. a store, its name, its address - are organized in dimension tables (`models/dimensions`), each with a unique identifier (primnary key). \
Each dimension table contains only attributed relative to the object they describe - the dimension store wont contain information relative to any possible customers as customers are entities in themselves.\
The goal is to normalise the data as much as possible to avoid redundancies - the more redundancies there are, the harder it is for stakeholders to navigate the warehouse and know from where to retrieve data.\
Besides the dimensions created, the model could have been more normalised by the creation of dimensions for countries, cities, status and dates.
The bridge tables(`models/bridges`) exist to establish the relationships between the dimensions unique keys, and how they  can be  used to buid the fact.\
The fact table (`models/facts`) is built on top of the dimensions; its unique identifier is the combination of the primary keys of the dimension tables. Ideally, this table should only contain aggregations at the granularity it describes, but for usability purposes, qualitative attributes like store and device details are added to simplify querying by stakeholders.\
In the fact table, considering the assumption that a transaction can be done to cover multiple products purchased, I split the transaction amount by all components of the primary key to make sure we don't end up accounting inflated transaction amounts. \
A simplified version of the schema can be seen in `dimensional_model.jpeg`

The tables for the dimensional model were build using a merge incremental strategy. Considering heavy daily loads of data, this reduces the cost to reprocess historical data, as it looks only into newly arriving rows (created_at) or rows that have been changed (happened_at) on the day the script is being executed. Considering lack of time/resources to orchestrate and schedule the pipeline for the case study, I setup a date (2022-10-01) as a variable to use as example.\
Similar to the staging layer, dbt tests were applied ton check for uniqueness of primary keys and non null values.

Considering an assumption that transaction data is updated and re-exported - ex. a same transaction id can have its status changed -, in order to keep the historical records for all transaction at all times, I created a snapshot of dim_transactions (`snapshots/dim_transactions_history.sql`). This keeps all values each row of the table has taken, with dbt_valid_from and dbt_valid_to as validity dates; the currently valid values, have dbt_valid_to as null.
<br/>
<br/>
#### Datamarts and solution queries
On top of the dimensional model, for usability's sake, I built 2 datamarts(`models/datamarts`): `SUMUP_TAKEHOMETASK.DATAMARTS.STORE_PERFORMANCE` and `SUMUP_TAKEHOMETASK.DATAMARTS.DEVICE_PERFORMANCE`; these are aggregations tables that answer specific stakeholder questions about a certain topic.\
Their goal is to be easy to use on visualization tools and simple to query with some basic sql knowledge.\
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
from SUMUP_TAKEHOMETASK.DATAMARTS.STORE_PERFORMANCE` 

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

For the datamarts and the queries above, I made  the assumption that, for a transaction to be a successful sale, its status needs  to be 'accepted'.
