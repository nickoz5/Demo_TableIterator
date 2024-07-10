from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from l0_ingest_from_source.config.ConfigStore import *
from l0_ingest_from_source.functions import *
from prophecy.utils import *
from l0_ingest_from_source.graph import *

def pipeline(spark: SparkSession) -> None:
    df_metadata_dataframe = metadata_dataframe(spark)
    df_define_source_target_names = define_source_target_names(spark, df_metadata_dataframe)
    TableIterator(Config.TableIterator).apply(spark, df_define_source_target_names)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("L0_Ingest_from_Source")\
                .getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/L0_Ingest_from_Source")
    registerUDFs(spark)
    
    MetricsCollector.instrument(spark = spark, pipelineId = "pipelines/L0_Ingest_from_Source", config = Config)(pipeline)

if __name__ == "__main__":
    main()
