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

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1766501506783 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="accelerometer_trusted_node1766501506783")

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1766501534265 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_trusted", transformation_ctx="step_trainer_trusted_node1766501534265")

# Script generated for node Joins
SqlQuery1502 = '''
select * 
from a
join s on a.timeStamp = s.sensorReadingTime;

'''
Joins_node1766501555815 = sparkSqlQuery(glueContext, query = SqlQuery1502, mapping = {"s":step_trainer_trusted_node1766501534265, "a":accelerometer_trusted_node1766501506783}, transformation_ctx = "Joins_node1766501555815")

# Script generated for node machine_learning_curated
EvaluateDataQuality().process_rows(frame=Joins_node1766501555815, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1766501431685", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
machine_learning_curated_node1766501779400 = glueContext.getSink(path="s3://stedi-project-unique/machine_learning_curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="machine_learning_curated_node1766501779400")
machine_learning_curated_node1766501779400.setCatalogInfo(catalogDatabase="stedi",catalogTableName="machine_learning_curated")
machine_learning_curated_node1766501779400.setFormat("json")
machine_learning_curated_node1766501779400.writeFrame(Joins_node1766501555815)
job.commit()