import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node Step_learner_trusted
Step_learner_trusted_node1746361373624 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_trusted ", transformation_ctx="Step_learner_trusted_node1746361373624")

# Script generated for node Accelerometer_trusted
Accelerometer_trusted_node1746361374727 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="Accelerometer_trusted_node1746361374727")

# Script generated for node Step Trainer Readings
SqlQuery3456 = '''
SELECT DISTINCT * 
FROM Step_learner_trusted
INNER JOIN Accelerometer_trusted on 
Accelerometer_trusted.timestamp=Step_learner_trusted.sensorreadingtime;
'''
StepTrainerReadings_node1746361491320 = sparkSqlQuery(glueContext, query = SqlQuery3456, mapping = {"Accelerometer_trusted":Accelerometer_trusted_node1746361374727, "Step_learner_trusted":Step_learner_trusted_node1746361373624}, transformation_ctx = "StepTrainerReadings_node1746361491320")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=StepTrainerReadings_node1746361491320, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1746360994951", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1746361629762 = glueContext.getSink(path="s3://stedi-survesh-s3/machine_learning/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1746361629762")
AmazonS3_node1746361629762.setCatalogInfo(catalogDatabase="stedi",catalogTableName="machine_learning_curated")
AmazonS3_node1746361629762.setFormat("json")
AmazonS3_node1746361629762.writeFrame(StepTrainerReadings_node1746361491320)
job.commit()