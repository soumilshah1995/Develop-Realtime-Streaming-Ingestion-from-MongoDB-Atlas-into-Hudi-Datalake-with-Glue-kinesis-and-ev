try:
    import sys
    from awsglue.transforms import *
    from awsglue.utils import getResolvedOptions

    from pyspark.sql.session import SparkSession
    from pyspark.context import SparkContext

    from awsglue.context import GlueContext
    from awsglue.job import Job

    from pyspark.sql import DataFrame, Row
    from pyspark.sql.functions import *
    from pyspark.sql.functions import col, to_timestamp, monotonically_increasing_id, to_date, when
    import datetime
    from awsglue import DynamicFrame
    import pyspark.sql.functions as F
    import boto3
except Exception as e:
    print("ERROR IMPORTS ", e)


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config('spark.serializer', 'org.apache.spark.serializer.KryoSerializer') \
        .getOrCreate()
    return spark


spark = create_spark_session()
sc = spark.sparkContext
glueContext = GlueContext(sc)

# ====================== Settings ============================================


db_name = "streams"
kinesis_table_name = 'kinesis_clicks_events'
table_name = "clicks"

record_key = 'fullDocument__id'
precomb = 'fullDocument_job_id'
s3_bucket = 'hudi-demos-emr-serverless-project-soumil'
s3_path_hudi = f's3a://{s3_bucket}/{table_name}/'
s3_path_spark = f's3://{s3_bucket}/spark_checkpoints/'

method = 'upsert'
table_type = "MERGE_ON_READ"

window_size = '10 seconds'
starting_position_of_kinesis_iterator = 'trim_horizon'

connection_options = {
    'hoodie.table.name': table_name,
    "hoodie.datasource.write.storage.type": table_type,
    'hoodie.datasource.write.recordkey.field': record_key,
    'hoodie.datasource.write.table.name': table_name,
    'hoodie.datasource.write.operation': method,
    'hoodie.datasource.write.precombine.field': precomb,

    'hoodie.datasource.hive_sync.enable': 'true',
    "hoodie.datasource.hive_sync.mode": "hms",
    'hoodie.datasource.hive_sync.sync_as_datasource': 'false',
    'hoodie.datasource.hive_sync.database': db_name,
    'hoodie.datasource.hive_sync.table': table_name,
    'hoodie.datasource.hive_sync.use_jdbc': 'false',
    'hoodie.datasource.hive_sync.partition_extractor_class': 'org.apache.hudi.hive.MultiPartKeysValueExtractor',
    'hoodie.datasource.write.hive_style_partitioning': 'true',

}

# ======================================================================


starting_position_of_kinesis_iterator = starting_position_of_kinesis_iterator

data_frame_DataSource0 = glueContext.create_data_frame.from_catalog(
    database=db_name,
    table_name=kinesis_table_name,
    transformation_ctx="DataSource0",
    additional_options={"inferSchema": "true", "startingPosition": starting_position_of_kinesis_iterator}
)


def flatten_df(nested_df, layers):
    """
    CREDITS https://stackoverflow.com/questions/38753898/how-to-flatten-a-struct-in-a-spark-dataframe
    """
    flat_cols = []
    nested_cols = []
    flat_df = []

    flat_cols.append([c[0] for c in nested_df.dtypes if c[1][:6] != 'struct'])
    nested_cols.append([c[0] for c in nested_df.dtypes if c[1][:6] == 'struct'])

    flat_df.append(nested_df.select(flat_cols[0] +
                                    [col(nc + '.' + c).alias(nc + '_' + c)
                                     for nc in nested_cols[0]
                                     for c in nested_df.select(nc + '.*').columns])
                   )
    for i in range(1, layers):
        print(flat_cols[i - 1])
        flat_cols.append([c[0] for c in flat_df[i - 1].dtypes if c[1][:6] != 'struct'])
        nested_cols.append([c[0] for c in flat_df[i - 1].dtypes if c[1][:6] == 'struct'])

        flat_df.append(flat_df[i - 1].select(flat_cols[i] +
                                             [col(nc + '.' + c).alias(nc + '_' + c)
                                              for nc in nested_cols[i]
                                              for c in flat_df[i - 1].select(nc + '.*').columns])
                       )

    return flat_df[-1]


def process_batch(data_frame, batchId):
    if (data_frame.count() > 0):
        kinesis_dynamic_frame = DynamicFrame.fromDF(data_frame, glueContext, "from_kinesis_data_frame")
        kinesis_spark_df = kinesis_dynamic_frame.toDF()

        print("##########")
        print("\n")
        print(kinesis_spark_df.show(3))
        print(kinesis_spark_df.printSchema())
        print("\n")

        kinesis_spark_df = kinesis_spark_df.select("detail.fullDocument")
        spark_df = flatten_df(kinesis_spark_df, 3)
        print("**************")
        print("\n")
        print(spark_df.show(3))
        print(spark_df.printSchema())
        print("\n")

        try:
            spark_df.write.format("hudi").options(**connection_options).mode("append").save(s3_path_hudi)
        except Exception as e:
            pass

glueContext.forEachBatch(
    frame=data_frame_DataSource0,
    batch_function=process_batch,
    options={
        "windowSize": window_size,
        "checkpointLocation": s3_path_spark
    }
)
