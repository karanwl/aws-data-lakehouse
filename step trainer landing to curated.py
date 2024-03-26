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

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1710302369239 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://glue-demo-karan/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="StepTrainerLanding_node1710302369239",
)

# Script generated for node Customer Curated
CustomerCurated_node1710302367281 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://glue-demo-karan/customer/curated/"],
        "recurse": True,
    },
    transformation_ctx="CustomerCurated_node1710302367281",
)

# Script generated for node SQL Query
SqlQuery44 = """
select * from stl where stl.serialnumber in (Select distinct(cc.serialnumber) from cc);
"""
SQLQuery_node1710302372172 = sparkSqlQuery(
    glueContext,
    query=SqlQuery44,
    mapping={
        "stl": StepTrainerLanding_node1710302369239,
        "cc": CustomerCurated_node1710302367281,
    },
    transformation_ctx="SQLQuery_node1710302372172",
)

# Script generated for node Amazon S3
AmazonS3_node1710302374930 = glueContext.getSink(
    path="s3://glue-demo-karan/step_trainer/trusted/",
    connection_type="s3",
    updateBehavior="LOG",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1710302374930",
)
AmazonS3_node1710302374930.setCatalogInfo(
    catalogDatabase="stedi_project", catalogTableName="step trainer landing to trusted"
)
AmazonS3_node1710302374930.setFormat("json")
AmazonS3_node1710302374930.writeFrame(SQLQuery_node1710302372172)
job.commit()
