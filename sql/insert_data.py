prepare_table = '''
select weather.prepare_weather_upsert(%(date_str)s)
'''

add_partition = '''
select weather.add_partition_weather(%(date)s::date)
'''

insert_weather = '''
 prepare_weather_upsert_{date_str}(%(countyid)s, %(date_record)s::timestamp, %(weather)s, %(current_temp)s,
%(high)s, %(low)s, %(precipitation_am)s, %(precipitation_pm)s, %(dew_point)s, %(wind_gust)s,
%(humidity_relh)s, %(slp)s, %(elevation)s, %(windchill)s, %(wind_degree)s, %(wind_speed)s, %(wind_direction)s)
'''

upsert = '''
insert into weather.weather_{date_str}(countyid, ts , Weather , Current_temp , high ,
    low , Precipitation_AM , Precipitation_PM , Dew_point, Wind_Gust ,
    Humidity_Relh , SLP , elevation  , WindChill , Wind_degree , Wind_speed  , Wind_Direction)
    values
    (%(countyid)s, %(date_record)s::timestamp, %(weather)s, %(current_temp)s,
%(high)s, %(low)s, %(precipitation_am)s, %(precipitation_pm)s, %(dew_point)s, %(wind_gust)s,
%(humidity_relh)s, %(slp)s, %(elevation)s, %(windchill)s, %(wind_degree)s, %(wind_speed)s, %(wind_direction)s)

    on conflict (ts, countyid)
    do  update set
    ( Weather , Current_temp , high ,
    low , Precipitation_AM , Precipitation_PM , Dew_point, Wind_Gust ,
    Humidity_Relh , SLP , elevation  , WindChill , Wind_degree , Wind_speed  , Wind_Direction) = (
            EXCLUDED.Weather,
            EXCLUDED.Current_temp,
            EXCLUDED.high,
            EXCLUDED.low,
            EXCLUDED.Precipitation_AM,
            EXCLUDED.Precipitation_PM,
            EXCLUDED.Dew_point,
            EXCLUDED.Wind_Gust,
            EXCLUDED.Humidity_Relh,
            EXCLUDED.SLP,
            EXCLUDED.elevation,
            EXCLUDED.WindChill,
            EXCLUDED.Wind_degree,
            EXCLUDED.Wind_speed,
            EXCLUDED.Wind_Direction

            )


'''