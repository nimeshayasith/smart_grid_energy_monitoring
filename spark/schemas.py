from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, BooleanType, TimestampType,
)

meter_schema = StructType([
    StructField("meter_id",               StringType(),    True),
    StructField("household_id",           StringType(),    True),
    StructField("power_consumption_kwh",  DoubleType(),    True),
    StructField("solar_generation_kwh",   DoubleType(),    True),
    StructField("grid_zone",              StringType(),    True),
    StructField("timestamp",              StringType(),    True),
])

tariff_schema = StructType([
    StructField("household_id",  StringType(),  True),
    StructField("tariff_rate",   DoubleType(),  True),
    StructField("billing_tier",  StringType(),  True),
    StructField("subsidy_flag",  BooleanType(), True),
])
