get_weather_tables = '''
SELECT c.relname weather_table
    FROM pg_catalog.pg_class c
	LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
	WHERE n.nspname = 'weather'
	AND c.relname like 'weather_2%' order by 1
'''

get_data = '''
SELECT * from weather.{table_name}
'''

drop_table = '''
drop table if exists  weather.{table_name}
'''