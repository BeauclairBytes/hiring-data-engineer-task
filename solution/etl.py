import duckdb
import clickhouse_connect


def _get_postgres_connection():
    con = duckdb.connect()
    pg_connection_str = "host=psql_source port=5432 user=postgres password=postgres"
    con.sql(f"ATTACH '{pg_connection_str}' AS postgres_db(TYPE postgres, SCHEMA 'public');")
    con.sql("SET TimeZone = 'UTC'")
    return con

def prepare_db_data(postgres_con, dw_filter_date=None):
    return postgres_con.execute("""
        WITH agg_clicks AS (
            SELECT
                campaign_id,
                created_at::date as date,
                COUNT(*) AS clicks
            FROM postgres_db.clicks
            WHERE created_at >=  COALESCE($1, created_at)
            GROUP BY 1,2
        ), agg_impressions AS (
            SELECT
                campaign_id,
                created_at::date as date,
                COUNT(*) AS impressions
            FROM postgres_db.impressions
            WHERE created_at >=  COALESCE($1, created_at)
            GROUP BY 1,2
        ), campaign_tmp AS (
            SELECT
                t.id AS campaign_id,
                t.advertiser_id,
                d.day::date AS date,
                t.budget/ DATEDIFF('day', start_date, end_date) as daily_budget,
                DATEDIFF('day', date, end_date) AS days_remaining,
                bid
            FROM
                postgres_db.campaign t,
                LATERAL generate_series(t.start_date, t.end_date, INTERVAL 1 DAY) AS d(day)
                WHERE created_at >=  COALESCE($1, created_at) OR 
                        updated_at >=  COALESCE($1, updated_at)
        ) 
        SELECT
            cp.campaign_id,
            cp.advertiser_id,
            cp.date AS date,
            DAY(cp.date) AS day,
            MONTH(cp.date) AS month,
            YEAR(cp.date) AS year,
            cp.days_remaining AS days_remaining,
            cp.bid AS bid,
            cp.daily_budget AS daily_budget,
            c.clicks AS daily_clicks,
            i.impressions AS daily_impressions,
            c.clicks / i.impressions AS ctr,
            NOW() as dw_load_time
        FROM campaign_tmp cp
        LEFT JOIN agg_clicks c ON cp.campaign_id = c.campaign_id AND cp.date=c.date
        LEFT JOIN agg_impressions i ON cp.campaign_id = i.campaign_id AND cp.date=i.date
    """, [dw_filter_date]).fetchdf()



def _get_clickhouse_connection():
    client = clickhouse_connect.get_client(
        host='ch_analytics',
        port=8123,
        username='admin',
        password='letmein123'
    )
    return client

def _create_clickhouse_schema(clickhouse_con):
    with open('ch_schema.sql') as ch_file:
        create_table_statements = ch_file.read()
        clickhouse_con.command(create_table_statements)


def _get_max_update_time(clickhouse_con):
    return clickhouse_con.query("SELECT MAX(dw_load_time) FROM campaign_performance_daily").result_rows[0][0]


def main():
    print("Starting connection and checking data in ClickHouse...")
    postgres_con = _get_postgres_connection()
    clickhouse_con = _get_clickhouse_connection()
    _create_clickhouse_schema(clickhouse_con)
    max_update_time = _get_max_update_time(clickhouse_con)
    campaign_df  = prepare_db_data(postgres_con, max_update_time)
    rows_to_insert = len(campaign_df)
    if rows_to_insert == 0:
        print("No new data available to update ClickHouse")
    else:
        print(f"Inserting {rows_to_insert} new rows in ClickHouse")
    clickhouse_con.insert_df('campaign_performance_daily', campaign_df)


if __name__ == "__main__":
    main()
