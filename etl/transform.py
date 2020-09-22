from data_catalog.catalog import catalog
from pyspark.sql import SparkSession
from pyspark.sql.functions import concat_ws, upper, from_unixtime, unix_timestamp, col


def clean_logs(spark: SparkSession, data_dir = catalog['clean/whitehouse_logs_combined']):
    """
    takes in a SparkSession object and file directory, and applies preliminary data cleaning ops

    :param spark: SparkSession object of cluster
    :param data_dir: path to file to be processed
    :return: Nothing
    """

    # load data from csv
    whl_df = spark.read.options(delimiter=',').csv(path=data_dir, header=True, inferSchema=True)
    print(whl_df.count(), 'records found before cleaning')

    # clean data
    whl_df = whl_df.dropna(how="any", subset=["NAMEFIRST", "NAMELAST", "APPT_START_DATE"])
    print(whl_df.count(), 'after dropping rows with missing name etc')

    # exclude cancelled appointments as well
    whl_df = whl_df.where(whl_df.APPT_CANCEL_DATE.isNull())
    print(whl_df.count(), 'after excluding canceled appointments')

    # load
    whl_df.write.mode('overwrite').csv(catalog['clean/whitehouse_logs_cleaned'], header=True)


def preprocess_names(spark: SparkSession, data_dir = catalog['clean/whitehouse_logs_cleaned']):

    whl_df = spark.read.options(delimiter=',').csv(path=data_dir, header=True, inferSchema=True)

    # created a concatenated Name string for visitor
    whl_df = whl_df.withColumn('VISITOR_NAME', upper(concat_ws(' ',
                                                         whl_df.NAMEFIRST,
                                                         whl_df.NAMEMID,
                                                         whl_df.NAMELAST)))

    # created a concatenated Name string for visitee
    whl_df = whl_df.withColumn('VISITEE_NAME',
                               upper(concat_ws(' ',
                                               whl_df.visitee_namefirst,
                                               whl_df.visitee_namelast)))
    # persist
    whl_df.write.\
        mode('overwrite').\
        csv(catalog['clean/whitehouse_logs_processed'], header=True)


def transform(spark: SparkSession):

    clean_logs(spark)

    preprocess_names(spark)


if __name__ == '__main__':
    spark = SparkSession.builder.appName('Whitehouse Logs').config('spark.master', 'local').getOrCreate()

    transform(spark)

"""
    TODOs:
        * Preprocess VISITEE Name to merge names like POTUS & "POTUS \"
"""

