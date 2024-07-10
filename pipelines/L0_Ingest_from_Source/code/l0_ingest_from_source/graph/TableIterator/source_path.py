from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from l0_ingest_from_source.functions import *

def source_path(spark: SparkSession) -> DataFrame:
    return spark.read.option("header", True).option("sep", ",").csv(Config.source_path)
