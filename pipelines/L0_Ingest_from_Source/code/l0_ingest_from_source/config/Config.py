from l0_ingest_from_source.graph.TenantIterator.config.Config import SubgraphConfig as TenantIterator_Config
from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(self, TenantIterator: dict=None, tenant_group: str=None, **kwargs):
        self.spark = None
        self.update(TenantIterator, tenant_group)

    def update(self, TenantIterator: dict={}, tenant_group: str="grp_000", **kwargs):
        prophecy_spark = self.spark
        self.TenantIterator = self.get_config_object(
            prophecy_spark, 
            TenantIterator_Config(prophecy_spark = prophecy_spark), 
            TenantIterator, 
            TenantIterator_Config
        )
        self.tenant_group = tenant_group
        pass
