# Importing Libraries

import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Bookings data

#Creating Streaming Table
@dlt.table(
    name = "stagging_bookings"
)
def stagging():
    df = spark.readStream.format('delta').load("/Volumes/workspace/bronze/bronzevolume/bookings/data/")
    return df


#Creating Streaming view

@dlt.view(
    name = "transform_bookings"
)
def transform():
    df = spark.readStream.table("stagging_bookings")
    df = df.drop("_rescued_data")
    df = df.withColumn("amount",col("amount").cast("double"))
    df = df.withColumn("modifiedDate",current_timestamp())

    return df


#Expectation rules
expect = {
    "rule1": "booking_id is NOT NULL",
    "rule2": "passenger_id is NOT NULL",
    "rule3": "flight_id is NOT NULL"
}


@dlt.table(
    name = "silver_bookings"
)
@dlt.expect_all(expect)
def silver():
    df = spark.readStream.table("transform_bookings")
    return df

##############################################################################

# Flights Data

# Creating view

@dlt.view(
    name = "transform_flights"
)
def transform():
    df = spark.readStream.format('delta').load("/Volumes/workspace/bronze/bronzevolume/flights/data/")
    df = df.drop("_rescued_data")
    df = df.withColumn("modifiedDate",current_timestamp())
    return df

# Creating an empty Streaming table
dlt.create_streaming_table("silver_flights")

# Slowly Changing Dimension - type 1
dlt.create_auto_cdc_flow(
    target = "silver_flights",
    source = "transform_flights",
    keys = ["flight_id"],
    sequence_by = col("modifiedDate"),
    stored_as_scd_type = 1
)

##############################################################################

# Passengers Data

# Creating view

@dlt.view(
    name = "transform_passengers"
)
def transform():
    df = spark.readStream.format('delta').load("/Volumes/workspace/bronze/bronzevolume/passengers/data/")
    df = df.drop("_rescued_data")
    df = df.withColumn("modifiedDate",current_timestamp())
    return df   

# Creating an empty Streaming table

dlt.create_streaming_table("silver_passengers")

# Slowly Changing Dimension - type 1
dlt.create_auto_cdc_flow(
    target = "silver_passengers",
    source = "transform_passengers",
    keys = ["passenger_id"],
    sequence_by = col("modifiedDate"),
    stored_as_scd_type = 1
)


##############################################################################

# Airports Data

# Creating view

@dlt.view(
    name = "transform_airports"
)
def transform():
    df = spark.readStream.format('delta').load("/Volumes/workspace/bronze/bronzevolume/airports/data/")
    df = df.drop("_rescued_data")
    df = df.withColumn("modifiedDate",current_timestamp())
    return df   

# Creating an empty Streaming table

dlt.create_streaming_table("silver_airports")

# Slowly Changing Dimension - type 1
dlt.create_auto_cdc_flow(
    target = "silver_airports",
    source = "transform_airports",
    keys = ["airport_id"],
    sequence_by = col("modifiedDate"),
    stored_as_scd_type = 1
)


##############################################################################

# Creating a Materialized view and joining all the streaming tables

@dlt.table(
    name = "silver_business"
)
def silver():
    df = dlt.readStream("silver_bookings")\
            .join(dlt.readStream("silver_flights"),["flight_id"])\
            .join(dlt.readStream("silver_passengers"),["passenger_id"])\
            .join(dlt.readStream("silver_airports"),["airport_id"])\
            .drop("modifiedDate")    
    return df


 