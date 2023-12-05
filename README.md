# Brightwheel exercise

This repository combines data from a variety of data sources to ingest prospective leads for enrichment, analysis and ultimately sales outreach. All of the data sources are external and have varying structures and data quality. The goal of this repository is to combine these data sources into one for deeper and easier analysis.

## Notes before we begin

* For this exercise, I initially attempted to connect to an S3 bucket to download the CSVs via the Python library boto3. The AWS Access Key Id I was given does not exist, so I had to use the CSVs included with the exercise instead.
* I performed some initial data exploration and observed that there were no duplicate records across the CSVs -- all records had a unique combination of phone number and address.
* Line 12 of the Nevada CSV begins with "MY LITTLE ANGEL�S". This � character does not play well with UTF-8 encoding, so I changed it manually to an apostrophe.
* The last record of the Oklahoma CSV is incomplete; it cuts off a string value before a second quotation mark denotes the end of the string, which causes an EOF error when the CSV is read. I went ahead and added a quotation mark at the end of the file, even though that last record is incomplete with no phone number or address.

## Install and run the service

```bash
git clone https://github.com/mlreese/brightwheel.git
cd brightwheel
```
If Postgres is not already installed locally, run the following command:

`brew install postgres`

Then create a local database named "brightwheel" with this command:

`createdb brightwheel`

After that, run the commands:

`python3 -m venv brightwheel`
`source brightwheel/bin/activate`

to create and activate a virtual environment. Then run the following commands:

`pip3 install --upgrade pip`
`pip3 install -r requirements.txt`

to upgrade pip and install all necessary packages.

Finally, run the command:

`python3 leads.py`

to run the leads.py file, which generates a Postgres table named "leads" that combines data from all of the CSV files.

## If I had more time

* I would do a deeper data discovery phase. There are 8 columns in the final table that contain all NULL values, and 6 of those (phone2, curriculum_type, language, license_renewed, title, and website_address) are because I was unsure of what kinds of data those columns should contain, if they should contain any values at all. The other two columns, max_age and min_age, could contain values from the Oklahoma CSV via parsing of the age columns (Ages Accepted 1, AA2, AA3, and AA4), but it would contain extremely complex regex functions that I just did not have the time to implement. I likewise made an educated guess on what the value of other columns should be, which I would get clarified and defined if I had more time.
* I would have tried to parse the Nevada CSV's singular "Address" column into multiple address dimensions (e.g. city, state, or zip.) Those values are not presented in any sort of uniform manner, so I believe it would be best to use GeoPy or the Google Maps API which can both take an address string input and parse it into its address elements.
* I would try to make the leads.py script a little DRY-er.
* I might have pivoted away from using pandas entirely. I figured it would be a good, simple way to do this, but there were so many schema and value transformations that perhaps using the native csv Python library or psycopg2 would have been better.

## Longer term ETL processes/strategies

I would use Airflow hosted on AWS (MWAA) for data orchestration, with this pipeline implemented as a DAG within Airflow, and the DAG would load the leads table into a Redshift data warehouse. The DAG would run once a week to make leads available for outreach on a weekly basis and would only load CSVs from the S3 bucket that were uploaded in the previous week. As the CSVs are fully refreshed with old and new records, the leads.py script would load this data into a staging table and compare it with existing records in the leads table to look for duplicate records; any duplicates would be removed from the staging table, and the remaining records would then be inserted into the leads table. I would use dbt to handle both downstream data transformations and all data validation tests. Downstream dbt model builds would also be run through Airflow DAGs so that I could effectively track the successful extraction, loading, and transformation of data end-to-end via chaining of Airflow operators or sensors all in Airflow. I enforced strict column names and datatypes in my leads.py script, so any column name or datatype changes in the CSVs would cause the data pipeline to fail; this is intentional and prevents our system from loading unclean data. These failures would surface in CloudWatch logs associated with MWAA, but, for more obvious alerting and monitoring, I would set up an integration with whatever company chat program we frequently use. For example, in the past, I have set up channels in Slack for Airflow notifications, such that when a DAG/data pipeline failed, Airflow would log into the Slack server and post a message about the DAG failure, with information such as the name of the DAG that failed and a link to the Airflow log that contains information about the failure. I could also get Airflow to send email notifications about DAG failures.

Furthermore, in this scenario, I would likely pivot leads.py to use more out-of-the-box Airflow functions for extracting and loading data, which are based on psycopg2 rather than pandas. Pandas is memory-intensive and inefficient, and scaling the number of source files could easily cause problems for it. I would prefer to parse the CSVs in this scenario using the Python csv library and load the data into Redshift via psycopg2.extras's execute_values function; combined, these would be orders of magnitude faster and more efficient than using pandas dataframes and pandas's to_sql function.
