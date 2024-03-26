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

# Script generated for node Amazon S3
AmazonS3_node1710299131833 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://glue-demo-karan/customer/landing/"],
        "recurse": True,
    },
    transformation_ctx="AmazonS3_node1710299131833",
)

# Script generated for node SQL Query
SqlQuery54 = """
select * from myDataSource where sharewithresearchasofdate is not null;
"""
SQLQuery_node1710299189443 = sparkSqlQuery(
    glueContext,
    query=SqlQuery54,
    mapping={"myDataSource": AmazonS3_node1710299131833},
    transformation_ctx="SQLQuery_node1710299189443",
)

# Script generated for node Amazon S3
AmazonS3_node1710299193774 = glueContext.getSink(
    path="s3://glue-demo-karan/customer/trusted/",
    connection_type="s3",
    updateBehavior="LOG",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1710299193774",
)
AmazonS3_node1710299193774.setCatalogInfo(
    catalogDatabase="stedi_project", catalogTableName="customer_landing_to_trusted"
)
AmazonS3_node1710299193774.setFormat("json")
AmazonS3_node1710299193774.writeFrame(SQLQuery_node1710299189443)
job.commit()
