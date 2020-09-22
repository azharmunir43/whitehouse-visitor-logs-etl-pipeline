import findspark

findspark.init()
from pyspark.sql import SparkSession
from etl.ingest import ingest_logs
from etl.transform import transform
from analytics.log_analytics import analysis

if __name__ == '__main__':
    spark = SparkSession.builder.appName('Whitehouse Logs').config('spark.master', 'local').getOrCreate()

    print(spark.version)

    ingest_logs(spark)

    transform(spark)

    analysis(spark)
