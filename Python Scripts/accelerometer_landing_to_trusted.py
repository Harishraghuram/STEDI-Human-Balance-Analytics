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

# Script generated for node accelerometer_landing
accelerometer_landing_node1766499518929 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_landing", transformation_ctx="accelerometer_landing_node1766499518929")

# Script generated for node customer_trusted
customer_trusted_node1766499437366 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="customer_trusted_node1766499437366")

# Script generated for node join
SqlQuery1451 = '''
select a.user,a.timeStamp,
a.x,a.y,a.z 
from a
join c
on a.user = c.email

'''
join_node1766499546679 = sparkSqlQuery(glueContext, query = SqlQuery1451, mapping = {"a":accelerometer_landing_node1766499518929, "c":customer_trusted_node1766499437366}, transformation_ctx = "join_node1766499546679")

# Script generated for node accelerometer_trusted
EvaluateDataQuality().process_rows(frame=join_node1766499546679, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1766499339408", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
accelerometer_trusted_node1766499660463 = glueContext.getSink(path="s3://stedi-project-unique/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="accelerometer_trusted_node1766499660463")
accelerometer_trusted_node1766499660463.setCatalogInfo(catalogDatabase="stedi",catalogTableName="accelerometer_trusted")
accelerometer_trusted_node1766499660463.setFormat("json")
accelerometer_trusted_node1766499660463.writeFrame(join_node1766499546679)
job.commit()