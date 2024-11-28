from pyspark.sql import SparkSession
from pyspark import SparkConf
import sys
import time
from pyspark.sql.functions import col, radians, cos, sin, acos, lit,min,max,avg,stddev,mean

def filterDataFrameByTimePeriod(df, timeMin, timeMax):
    return df.filter((col("timestep_time") >= timeMin) & (col("timestep_time") <= timeMax))

def calculateDistanceAndFilter(df, latitude_point, longitude_point, proximity_size):

    point_rad_lat = radians(lit(latitude_point))
    point_rad_long = radians(lit(longitude_point))

    df = df.withColumn(
        "distance",
        lit(6371) * acos(
            sin(point_rad_lat) * sin(radians(col("vehicle_x"))) +
            cos(point_rad_lat) * cos(radians(col("vehicle_x"))) * cos(radians(col("vehicle_y")) - point_rad_long)
        )
    )
    return df.filter(col("distance") <= proximity_size).dropDuplicates(["vehicle_id"])


def calculateStatistics(df_filtered, columns):
    
    for column in columns:
        stats_df = df_filtered.groupBy("vehicle_lane").agg(
            min(column).alias("min_" + column),
            max(column).alias("max_" + column),
            avg(column).alias("avg_" + column),
            stddev(column).alias("stddev_" + column),
            mean(column).alias("mean_" + column)
        )
        print(f"Statistics for {column} by lane:")
        stats_df.show()

def task1(spark, args):

    timeTask1 = time.time()

    data_path, latitude_point, longitude_point, proximity_size, timeMin, timeMax = args[:6]
    vehicle_type = None if len(args) < 7 else args[6]  
    
    df = spark.read.option("header", "true").option("delimiter", ";").csv(data_path, inferSchema=True)

    df_filtered = filterDataFrameByTimePeriod(df, timeMin, timeMax)

    if vehicle_type:  
        df_filtered = df_filtered.filter(col("vehicle_type") == vehicle_type)

    df_filtered = calculateDistanceAndFilter(df_filtered, float(latitude_point), float(longitude_point), float(proximity_size))
    
    num_vehicles = df_filtered.count()
    print(f"Number of nearby vehicles: {num_vehicles}")
    df_filtered.show()

    return time.time() - timeTask1

def task2(spark, args):

    timeTask2 = time.time()

    data_path, timeMin, timeMax, task = args
    
    df = spark.read.option("header", "true").option("delimiter", ";").csv(data_path, inferSchema=True)
    df_filtered = filterDataFrameByTimePeriod(df, timeMin, timeMax)

    pollution_columns = ["vehicle_CO", "vehicle_CO2", "vehicle_HC", "vehicle_NOx", "vehicle_PMx", "vehicle_noise"]
    fuel_columns = ["vehicle_electricity", "vehicle_fuel"]
    
    columns = pollution_columns if task == 'pollution' else fuel_columns

    calculateStatistics(df_filtered,columns)

    return time.time() - timeTask2

if __name__ == "__main__":

    timeApp = time.time()

    if len(sys.argv) < 5:
        print("Fix arguments.")
        sys.exit(-1)
    
    appName="Project1"

    conf = SparkConf()
    conf.setMaster("spark://spark-master:7077")
    #conf.setMaster("local")

    spark = SparkSession.builder.config(conf=conf).appName(appName).getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    task_indicator = sys.argv[1]

    timeFun=0

    if task_indicator == 'task1':
        task1_args = sys.argv[2:]  
        timeFun=task1(spark, task1_args)
    elif task_indicator == 'task2':
        task2_args = sys.argv[2:]  
        timeFun=task2(spark, task2_args)
    else:
        print("No task:", task_indicator)
        sys.exit(-1)

    spark.stop()

    print(f"Time for {sys.argv[1]}: {timeFun}")
    print(f"Time for app: {time.time()-timeApp}")
