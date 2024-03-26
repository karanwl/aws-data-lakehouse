import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1710300151966 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://glue-demo-karan/accelerometer/landing/"],
        "recurse": True,
    },
    transformation_ctx="AccelerometerLanding_node1710300151966",
)

# Script generated for node Customer Trusted
CustomerTrusted_node1710300353016 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://glue-demo-karan/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="CustomerTrusted_node1710300353016",
)

# Script generated for node SQL Query
SqlQuery62 = """
select * from accelerometer_landing al where al.user in (Select Distinct(customer_trusted.email) from customer_trusted)
"""
SQLQuery_node1710300170095 = sparkSqlQuery(
    glueContext,
    query=SqlQuery62,
    mapping={
        "accelerometer_landing": AccelerometerLanding_node1710300151966,
        "customer_trusted": CustomerTrusted_node1710300353016,
    },
    transformation_ctx="SQLQuery_node1710300170095",
)

# Script generated for node Amazon S3
AmazonS3_node1710300173719 = glueContext.getSink(
    path="s3://glue-demo-karan/accelerometer/trusted/",
    connection_type="s3",
    updateBehavior="LOG",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1710300173719",
)
AmazonS3_node1710300173719.setCatalogInfo(
    catalogDatabase="stedi_project", catalogTableName="accelerometer landing to trusted"
)
AmazonS3_node1710300173719.setFormat("json")
AmazonS3_node1710300173719.writeFrame(SQLQuery_node1710300170095)
job.commit()
