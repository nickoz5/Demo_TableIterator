from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from l0_ingest_from_source.config.ConfigStore import *
from l0_ingest_from_source.functions import *

def define_source_target_names(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(col("tenant_id").alias("source_catalog"))
