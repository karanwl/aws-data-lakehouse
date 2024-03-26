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

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1710301264425 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://glue-demo-karan/accelerometer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="AccelerometerTrusted_node1710301264425",
)

# Script generated for node Customer Trusted
CustomerTrusted_node1710301260543 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://glue-demo-karan/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="CustomerTrusted_node1710301260543",
)

# Script generated for node SQL Query
SqlQuery94 = """
select * from ct where ct.email in (Select distinct(at.user) from at);
"""
SQLQuery_node1710301284197 = sparkSqlQuery(
    glueContext,
    query=SqlQuery94,
    mapping={
        "at": AccelerometerTrusted_node1710301264425,
        "ct": CustomerTrusted_node1710301260543,
    },
    transformation_ctx="SQLQuery_node1710301284197",
)

# Script generated for node Amazon S3
AmazonS3_node1710301288408 = glueContext.getSink(
    path="s3://glue-demo-karan/customer/curated/",
    connection_type="s3",
    updateBehavior="LOG",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1710301288408",
)
AmazonS3_node1710301288408.setCatalogInfo(
    catalogDatabase="stedi_project",
    catalogTableName="customer trusted to customer curated",
)
AmazonS3_node1710301288408.setFormat("json")
AmazonS3_node1710301288408.writeFrame(SQLQuery_node1710301284197)
job.commit()
