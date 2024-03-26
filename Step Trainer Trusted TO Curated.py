import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Accelerometer trusted
Accelerometertrusted_node1710303657292 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://glue-demo-karan/accelerometer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="Accelerometertrusted_node1710303657292",
)

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1710304233035 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://glue-demo-karan/step_trainer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="StepTrainerTrusted_node1710304233035",
)

# Script generated for node Customer Curated
CustomerCurated_node1710303655773 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://glue-demo-karan/customer/curated/"],
        "recurse": True,
    },
    transformation_ctx="CustomerCurated_node1710303655773",
)

# Script generated for node AccT + Cc
AccTCc_node1710303661807 = Join.apply(
    frame1=Accelerometertrusted_node1710303657292,
    frame2=CustomerCurated_node1710303655773,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="AccTCc_node1710303661807",
)

# Script generated for node Join
AccTCc_node1710303661807DF = AccTCc_node1710303661807.toDF()
StepTrainerTrusted_node1710304233035DF = StepTrainerTrusted_node1710304233035.toDF()
Join_node1710304290603 = DynamicFrame.fromDF(
    AccTCc_node1710303661807DF.join(
        StepTrainerTrusted_node1710304233035DF,
        (
            AccTCc_node1710303661807DF["serialnumber"]
            == StepTrainerTrusted_node1710304233035DF["serialnumber"]
        )
        & (
            AccTCc_node1710303661807DF["timestamp"]
            == StepTrainerTrusted_node1710304233035DF["sensorreadingtime"]
        ),
        "right",
    ),
    glueContext,
    "Join_node1710304290603",
)

# Script generated for node Drop Fields
DropFields_node1710305187171 = DropFields.apply(
    frame=Join_node1710304290603,
    paths=["phone", "email"],
    transformation_ctx="DropFields_node1710305187171",
)

# Script generated for node Amazon S3
AmazonS3_node1710303664280 = glueContext.getSink(
    path="s3://glue-demo-karan/machine_learning/curated/",
    connection_type="s3",
    updateBehavior="LOG",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1710303664280",
)
AmazonS3_node1710303664280.setCatalogInfo(
    catalogDatabase="stedi_project", catalogTableName="Step Trainer Trusted TO Curated"
)
AmazonS3_node1710303664280.setFormat("json")
AmazonS3_node1710303664280.writeFrame(DropFields_node1710305187171)
job.commit()
