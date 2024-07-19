from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from l0_ingest_from_source.config.ConfigStore import *
from l0_ingest_from_source.functions import *

def metadata_dataframe(spark: SparkSession) -> DataFrame:
    out0 = spark.table("lakehouse.control.tenant").filter("group = 'grp_701' AND enabled=true")

    return out0
