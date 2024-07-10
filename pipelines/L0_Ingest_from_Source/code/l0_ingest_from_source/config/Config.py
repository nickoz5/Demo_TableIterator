from l0_ingest_from_source.graph.TableIterator.config.Config import SubgraphConfig as TableIterator_Config
from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(self, TableIterator: dict=None, **kwargs):
        self.spark = None
        self.update(TableIterator)

    def update(self, TableIterator: dict={}, **kwargs):
        prophecy_spark = self.spark
        self.TableIterator = self.get_config_object(
            prophecy_spark, 
            TableIterator_Config(prophecy_spark = prophecy_spark), 
            TableIterator, 
            TableIterator_Config
        )
        pass
