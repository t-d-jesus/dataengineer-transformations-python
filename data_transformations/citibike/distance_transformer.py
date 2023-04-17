from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, udf, col
from pyspark.sql.types import DoubleType
from haversine import haversine, Unit

METERS_PER_FOOT = 0.3048
FEET_PER_MILE = 5280
EARTH_RADIUS_IN_METERS = 6371e3
METERS_PER_MILE = METERS_PER_FOOT * FEET_PER_MILE


@udf(returnType=DoubleType())
def distance(start_station_latitude, start_station_longitude,end_station_latitude,end_station_longitude):

    lat_long1 = (start_station_latitude,start_station_longitude)
    lat_long2 = (end_station_latitude,end_station_longitude)
    distance_calculation = haversine(lat_long1, lat_long2, unit=Unit.MILES)

    return round(distance_calculation, 2)

def compute_distance(_spark: SparkSession, dataframe: DataFrame) -> DataFrame:
    print('--------------->--------------->compute_distance--------------->--------------->')
    dataframe.show()
    dataframe.printSchema()
    dataframe = dataframe.withColumn("distance", distance(col("start_station_latitude"),
                                              col('start_station_longitude'),
                                              col('end_station_latitude'),
                                              col('end_station_longitude')
                                              ))

    dataframe.show()
    print('========>========>========>========>========>========>')

    return dataframe


def run(spark: SparkSession, input_dataset_path: str, transformed_dataset_path: str) -> None:
    input_dataset = spark.read.parquet(input_dataset_path)
    input_dataset.show()

    dataset_with_distances = compute_distance(spark, input_dataset)
    dataset_with_distances.show()

    dataset_with_distances.write.parquet(transformed_dataset_path, mode='append')
