-- Create prepare statement

create or replace function prepare_weather_upsert(date_str varchar)
RETURNS varchar as

$PREPARE_NAME$

DECLARE
prepare_name varchar := 'prepare_weather_upsert_' || date_str;
date_str varchar := date_str;
table_exists integer;

Begin
select count(*)
INTO table_exists
from pg_prepared_statements where name = prepare_name;

if table_exists = 0 Then
EXECUTE ' PREPARE ' ||prepare_name|| ' as
    insert into geo.gps_'||date_str||'(countyid, ts , Weather , Current_temp , high ,
    low , Precipitation_AM , Precipitation_PM , Dew_point, Wind_Gust ,
    Humidity_Relh , SLP , elevation  , WindChill , Wind_degree , Wind_speed  , Wind_Direction)
    values
    ( $1, $2, $3, $4, $5,
    $6, $7, $8, $9,$10,
     $11, $12, $13, $14, $15, $16, $17 )

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

            )';

end if;
RETURN prepare_name;
END;
$PREPARE_NAME$
 LANGUAGE plpgsql VOLATILE
COST 100;
GRANT EXECUTE ON FUNCTION prepare_weather_upsert(varchar) TO public;