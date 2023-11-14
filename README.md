# Creating Data Pipelines in Airflow 
Example of implementing data pipelines for ingesting 3rd party data into a Snowflake database

The code corresponds to my article [Creating Data Pipelines in Airflow](https://medium.com/@davorborcic/creating-snowflake-data-pipelines-in-airflow-26c6dd98e03d)
The best way to understand the code and its purpose is to go through the article.

## Prerequisites
- Access to [NBA_ELO statistics file](https://raw.githubusercontent.com/fivethirtyeight/data/master/nba-elo/nbaallelo.csv)
- Access to Snowflake (there is a free trial option available)

## Instructions
1. Clone the repository
2. Download the NBA file and save it as `nba_elo.csv` in `files\staging` subfolder
3. Connect to the Snowflake and run `create_database_objects.sql`

## Start the Docker, configuring Airflow, and initiating the data pipeline
1. From `data_pipelines` folder execute `docker compose up`
2. Log in to https://localhost:8080 (airflow / airflow)
3. Create Snowflake connection named `sf1`
4. Enable DAG
5. Move or copy the `nba_elo.csv` from `staging` subfolder into `files` folder
6. Observe the data pipeline running and loading the data into the database










### Credits

- [Running Airflow in a Docker, Apache Airflow](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html)
- [Data Pipelines with Apache Airflow, Manning Press, 2021](https://www.manning.com/books/data-pipelines-with-apache-airflow)





