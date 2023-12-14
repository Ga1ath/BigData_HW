from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from math import radians, sin, cos, sqrt, atan2


def calculate_distance(latitude1, longitude1, latitude2, longitude2):
    latitude1, longitude1, latitude2, longitude2 = map(radians, [latitude1, longitude1, latitude2, longitude2])

    a = sin((latitude2 - latitude1) / 2) ** 2 + \
        cos(latitude1) * cos(latitude2) * sin((longitude2 - longitude1) / 2) ** 2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))

    return 6371 * c


def print_result(source_data_array):
    for item in source_data_array:
        print(f"{item[0]}: {item[1]} km")
    print("--------------------------------------\n")


spark_session = SparkSession(SparkContext("local", "HomeWork 1.1"))
schema = StructType([
    StructField("ID", IntegerType()),
    StructField("Name", StringType()),
    StructField("global_id", IntegerType()),
    StructField("IsNetObject", StringType()),
    StructField("OperatingCompany", StringType()),
    StructField("TypeObject", StringType()),
    StructField("AdmArea", StringType()),
    StructField("District", StringType()),
    StructField("Address", StringType()),
    StructField("PublicPhone", StringType()),
    StructField("SeatsCount", IntegerType()),
    StructField("SocialPrivileges", StringType()),
    StructField("Longitude_WGS84", DoubleType()),
    StructField("Latitude_WGS84", DoubleType()),
    StructField("geoData", StringType())
])
data = spark_session.read.csv("places.csv", header=False, schema=schema)
target_latitude, target_longitude = 55.751244, 37.618423

print("Расстояние от заданной точки (lat=55.751244, lng=37.618423) до каждого заведения общепита из набора данных")
print_result(
    data.rdd.map(
        lambda row: (
            row["Name"],
            calculate_distance(row["Latitude_WGS84"], row["Longitude_WGS84"], target_latitude, target_longitude)
        )
    ).take(10)
)

all_distances = data.rdd.cartesian(data.rdd).filter(
    lambda x: x[0]["ID"] < x[1]["ID"]
).map(
    lambda x: (
        (x[0]["Name"], x[1]["Name"]),
        calculate_distance(
            x[0]["Latitude_WGS84"],
            x[0]["Longitude_WGS84"],
            x[1]["Latitude_WGS84"],
            x[1]["Longitude_WGS84"]
        )
    )
)

print("Pасстояние между всеми заведениями общепита из набора данных")
print_result(all_distances.take(10))

print("Топ-10 наиболее близких и наиболее отдаленных заведений")
print_result(
    all_distances.takeOrdered(10, key=lambda x: x[1]) + \
    all_distances.takeOrdered(10, key=lambda x: -x[1])
)

spark_session.stop()
