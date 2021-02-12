database = 'test_data'
datasets = ['monthly_sunspots', 'metro_traffic_ts']
# datasets = ['monthly_sunspots', ]
tables = {
    'monthly_sunspots': f"""
    CREATE TABLE IF NOT EXISTS {database}.monthly_sunspots (
        Month Date,
        Sunspots Float64
    ) ENGINE = MergeTree()
    ORDER BY Month
    PARTITION BY Month
    """,
    'metro_traffic_ts': f"""
    CREATE TABLE IF NOT EXISTS {database}.metro_traffic_ts (
        holiday String,
        temp Float64,
        rain_1h Float64,
        snow_1h Float64,
        clouds_all UInt8,
        weather_main String,
        weather_description String,
        date_time DateTime('UTC'),
        traffic_volume Int16
    ) ENGINE = MergeTree()
    ORDER BY traffic_volume
    PARTITION BY weather_main
    """
}
