CREATE EXTERNAL TABLE IF NOT EXISTS forecast_weather_sp (
  dt_txt TIMESTAMP,
    main STRUCT<
    temp: DOUBLE, 
    feels_like: DOUBLE, 
    temp_min: DOUBLE, 
    temp_max: DOUBLE, 
    pressure: INT, 
    humidity: INT>,
  weather ARRAY<STRUCT<
    id: INT, 
    main: STRING, 
    description: STRING, 
    icon: STRING>>,
  wind STRUCT<speed: DOUBLE, deg: INT>,
  clouds STRUCT<all: INT>,
  dt INT
)
PARTITIONED BY (year INT, month INT, day INT)
STORED AS PARQUET
LOCATION 's3://weather-forecast-teste-itau/forecast/';

MSCK REPAIR TABLE forecast_weather_sp;