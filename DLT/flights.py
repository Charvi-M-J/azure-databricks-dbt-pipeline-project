import dlt
from pyspark.sql.functions import col

# ==========================
# FLIGHTS
# ==========================

@dlt.view(name="trans_flights")
def trans_flights():

    return (
        spark.readStream
            .format("delta")
            .load("/Volumes/workspace/bronze/bronzevolume/flights/data")
    )

dlt.create_streaming_table(name="silver_flights")

dlt.create_auto_cdc_flow(
    target="silver_flights",
    source="trans_flights",
    keys=["flight_id"],
    sequence_by=col("flight_id"),
    stored_as_scd_type=1
)

# ==========================
# PASSENGERS
# ==========================

@dlt.view(name="trans_passengers")
def trans_passengers():

    return (
        spark.readStream
            .format("delta")
            .load("/Volumes/workspace/bronze/bronzevolume/customers/data")
    )

dlt.create_streaming_table(name="silver_passengers")

dlt.create_auto_cdc_flow(
    target="silver_passengers",
    source="trans_passengers",
    keys=["passenger_id"],
    sequence_by=col("passenger_id"),
    stored_as_scd_type=1
)

# ==========================
# AIRPORTS
# ==========================

@dlt.view(name="trans_airports")
def trans_airports():

    return (
        spark.readStream
            .format("delta")
            .load("/Volumes/workspace/bronze/bronzevolume/airports/data")
    )

dlt.create_streaming_table(name="silver_airports")

dlt.create_auto_cdc_flow(
    target="silver_airports",
    source="trans_airports",
    keys=["airport_id"],
    sequence_by=col("airport_id"),
    stored_as_scd_type=1
)