from pyspark.sql import SparkSession
from data_catalog.catalog import catalog
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql.functions import input_file_name, from_unixtime, unix_timestamp, col


def ingest_logs(spark: SparkSession, data_dir = catalog['landing/whitehouse_logs']):
    """
        takes in SparkSession object and a path to csv file, and load data

    :param spark: SparkSession object
    :param data_dir: directory of csv files
    :return: None
    """

    # define schema, ideally we w'd want to have control over schem
    # schema = StructType([
    #     StructField('NAMELAST', StringType(), nullable=True),
    #     StructField('NAMEFIRST', StringType(), nullable=True),
    #     StructField('NAMEMID', StringType(), nullable=True),
    #     StructField('APPT_START_DATE', TimestampType(), nullable=True),
    #     StructField('APPT_CANCEL_DATE', TimestampType(), nullable=True),
    #     StructField('visitee_namelast', StringType(), nullable=True),
    #     StructField('visitee_namefirst', StringType(), nullable=True),
    #     ...
    # ])

    # load raw csv data using inferSchema for now
    logs_df = spark.read.\
        options(delimiter=',').csv(path=data_dir, header=True, inferSchema=True).\
        withColumn('FILENAME', input_file_name())


    logs_df.printSchema()

    # convert to timestamp
    logs_df = logs_df.withColumn('APPT_START_DATE',
                               from_unixtime(unix_timestamp(logs_df.APPT_START_DATE, "MM/dd/yy HH:mm")))

    logs_df.show(20)

    print(logs_df.count(), 'records found')

    # write data combined as one
    logs_df.write.mode('overwrite').csv(catalog['clean/whitehouse_logs_combined'], header=True)


if __name__=='__main__':

    spark = SparkSession.builder.appName('Whitehouse Logs').config('spark.master', 'local').getOrCreate()

    ingest_logs(spark)
