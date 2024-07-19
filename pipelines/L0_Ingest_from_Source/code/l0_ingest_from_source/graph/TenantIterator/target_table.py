from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from l0_ingest_from_source.functions import *

def target_table(spark: SparkSession, in0: DataFrame):
    in0.write\
        .format("delta")\
        .mode("overwrite")\
        .saveAsTable("`nick_work`.`default`.`{}`".format(f"nick_test_pipeline_{Config.source_catalog}"))
