# Solution Documentation
### ClickHouse Schema

ClickHouse contains one fact table modeled similarly to Kimball's architecture with daily campaign data. Besides the requestes KPIs, I also added ``days_remaining`` and ``daily_budget`` which could be useful is data about spendings is also available


### Data Pipeline


The pipeline runs in Python and uses [duckdb](https://duckdb.org/) to connect to Postgres, load the data into a [Pandas](https://pandas.pydata.org/) dataframe and later on load the data into ClickHouse by using the official ClickHouse Client for Python. 

For incremental pipelines, it checks the last load time into ClickHouse to filter the old data and get only the new/updated campaigns, clicks and impressions


### How to Run

The ETL is embedded in the docker-compose script. It will automatically be triggered once you start the containers with the following command

```bash
docker-compose up -d
```

The etl container will wait for Postgres and ClickHouse to start. In case you want to trigger the etl separately (e.g., for an incremental run), you can use the following command:


```bash 
docker-compose up etl
```


### Queries

The requested queries are saved in the queries.sql and can easily be tested in the ClickHouse UI.

### Known Issues

The script that generates data is creating new campaigns in UTC timezone, while the clicks and impressions have the local timestamp.