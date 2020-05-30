import configparser
import os
from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.ml import Pipeline
from pyspark.sql.types import IntegerType
from pyspark.ml.feature import OneHotEncoderEstimator, StringIndexer, VectorAssembler


def create_spark_context():
    """Creates Spark session, connects to Amazon Redshift and returns SQLContext

    Returns:
        SQLContext
    """
    config = configparser.ConfigParser()
    config.read('dl.cfg')

    os.environ['aws_access_key'] = config['CREDS']['aws_access_key']
    os.environ['aws_secret_key'] = config['CREDS']['aws_secret_key']
    os.environ['aws_region'] = config['CREDS']['aws_region']
    os.environ['aws_bucket'] = config['CREDS']['aws_bucket']
    os.environ['redshift_user'] = config['CREDS']['redshift_user']
    os.environ['redshift_pass'] = config['CREDS']['redshift_pass']
    os.environ['redshift_host'] = config['CREDS']['redshift_host']

    jars = [
        "jars/postgresql-42.2.6.jar"
    ]
    conf = (
        SparkConf()
        .setAppName("S3 with Redshift")
        .set("spark.driver.extraClassPath", ":".join(jars))
        .set("spark.hadoop.fs.s3a.access.key", os.environ['aws_access_key'])
        .set("spark.hadoop.fs.s3a.secret.key", os.environ['aws_secret_key'])
        .set("spark.hadoop.fs.s3a.path.style.access", True)
        .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .set("com.amazonaws.services.s3.enableV4", True)
        .set("spark.hadoop.fs.s3a.endpoint", f"s3-{os.environ['aws_region']}.amazonaws.com")
        .set("spark.executor.extraJavaOptions", "-Dcom.amazonaws.services.s3.enableV4=true")
        .set("spark.driver.extraJavaOptions", "-Dcom.amazonaws.services.s3.enableV4=true")
    )
    sc = SparkContext(conf=conf).getOrCreate()
    sqlContext = SQLContext(sc)

    return sqlContext


def read_table(sqlContext, table):
    """Read the table data from Amazon Redshift and returns a Dataframe

    Args:
        sqlContext: SQLContext of Spark Session
        table: Redshift table

    Returns:
        Dataframe: Dataframe with data from Redshift table
    """
    df = sqlContext.read.format("jdbc").option("url", f"jdbc:postgresql://{os.environ['redshift_host']}"). \
        option("dbtable", table).option("user", os.environ['redshift_user']). \
        option("password", os.environ['redshift_pass']).load()
    return df


def transform_df(df, train_pc, test_pc):
    """Given the dataframe, categorize and one-hot encode the categorical columns, append
    the numerical columns, split data into training and test datasets

    Args:
        df: Dataframe
        train_pc: Percentage of train data
        test_pc: Percentage of test data

    Returns:
        train: train dataset
        test: test dataset
    """
    cols = df.columns
    df = df.withColumn('seed', df.seed.substr(2, 3).cast(IntegerType()))
    df = df.withColumn('result', df.result.cast(IntegerType()))

    categoricalColumns = ['loc']
    stages = []
    for categoricalCol in categoricalColumns:
        stringIndexer = StringIndexer(
            inputCol=categoricalCol, outputCol=categoricalCol + 'Index')
        encoder = OneHotEncoderEstimator(inputCols=[stringIndexer.getOutputCol()],
                                         outputCols=[categoricalCol + "classVec"])
        stages += [stringIndexer, encoder]
    label_stringIdx = StringIndexer(
        inputCol='result', outputCol='result_label')
    stages += [label_stringIdx]
    numericCols = ['season', 'teamid', 'oppteamid', 'score', 'numot',
                   'fgm', 'fga', 'fgm3', 'fga3', 'ftm', 'fta', 'or', 'dr', 'ast', 'to',
                   'stl', 'blk', 'pf', 'cityid', 'seed']
    assemblerInputs = [
        c + "classVec" for c in categoricalColumns] + numericCols
    assembler = VectorAssembler(
        inputCols=assemblerInputs, outputCol="features")
    stages += [assembler]

    pipeline = Pipeline(stages=stages)
    pipelineModel = pipeline.fit(df)
    df = pipelineModel.transform(df)
    selectedCols = ['result_label', 'features'] + cols
    df = df.select(selectedCols)

    train, test = df.randomSplit([train_pc, test_pc], seed=1)
    return train, test


if __name__ == '__main__':
    sc = create_spark_context()
    df = read_table(sc, "public.resultssummary")
    train, test = transform_df(df, 0.7, 0.3)
