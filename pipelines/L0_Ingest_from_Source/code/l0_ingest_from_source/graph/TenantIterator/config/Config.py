from prophecy.config import ConfigBase


class SubgraphConfig(ConfigBase):

    def __init__(self, prophecy_spark=None, source_catalog: str="", **kwargs):
        self.source_catalog = source_catalog
        pass

    def update(self, updated_config):
        self.source_catalog = updated_config.source_catalog
        pass

Config = SubgraphConfig()
