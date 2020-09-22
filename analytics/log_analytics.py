from data_catalog.catalog import catalog
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import countDistinct, col, count


def frequent_visitors(spark_df: DataFrame, n=10, export=True):
    """
    takes a dataframe containing visitor logs and computes n most frequent visitors

    :param spark_df: Spark Dataframe
    :param n: number of top visitors
    :return: pandas dataframe with freq visitors
    """

    # compute aggregate and convert to pandas for visualization
    fre_visitors = spark_df.groupBy(['VISITOR_NAME']).agg(\
        count('APPT_START_DATE').alias('Visits') # countDistinct could have been used depending on business
        ).\
        orderBy('Visits', ascending=False).\
        limit(n).\
        toPandas()

    print(fre_visitors)

    # persist
    if export:
        fre_visitors.to_csv(catalog['business/frequent_visitors'], index=False)

    return fre_visitors


def frequent_visitees(spark_df: DataFrame, n=10, export=True):
    """
    takes a dataframe containing visitor logs and computes n most frequent visitees

    :param spark_df: Spark Dataframe
    :param n: number of top visitors
    :return: pandas dataframe with freq visitees
    """

    # compute aggregate and convert to pandas for visualization
    fre_visitees = spark_df.groupBy(['VISITEE_NAME']).agg( \
        count('APPT_START_DATE').alias('Visits')
    ). \
        orderBy('Visits', ascending=False). \
        limit(n). \
        toPandas()

    print(fre_visitees)
    # persist
    if export:
        fre_visitees.to_csv(catalog['business/frequent_visitees'], index=False)

    return fre_visitees


def frequent_combinations(spark_df: DataFrame, n=10, export=True):
    """
    takes a dataframe containing visitor logs and computes n most frequent visitor-visite pairs

    :param spark_df: Spark Dataframe
    :param n: number of top visitors
    :return: pandas dataframe with visitor-visite pairs
    """

    # compute aggregate and convert to pandas for visualization
    freq_pairs = spark_df.groupBy(['VISITOR_NAME', 'VISITEE_NAME']).agg( \
        count('APPT_START_DATE').alias('Visits')
    ). \
        orderBy('Visits', ascending=False). \
        limit(n). \
        toPandas()

    print(freq_pairs)
    # persist
    if export:
        freq_pairs.to_csv(catalog['business/frequent_pairs'], index=False)

    return freq_pairs


def analysis(spark: SparkSession, data_dir = catalog['clean/whitehouse_logs_processed']):
    whl_df = spark.read.options(delimiter=',').csv(path=data_dir, header=True, inferSchema=True)



    frequent_visitors(whl_df)
    frequent_visitees(whl_df)
    frequent_combinations(whl_df)


if __name__ == '__main__':
    spark = SparkSession.builder.appName('Whitehouse Logs').config('spark.master', 'local').getOrCreate()

    analysis(spark)
