# Table Iterator Demo
This sample pipeline demonstrates Prophecy's ability to iterate over one or more Gems for each row for a given input DataFrame. The input DataFrame can either a raw file, a table, or even a python script. Please see [here](https://docs.prophecy.io/Spark/gems/subgraph/table-iterator/) for the full TableIterator documentation.

## Pipelines

1. **L0_Ingest_from_Source**
The "L0_Ingest_from_Source" Spark pipeline is responsible for ingesting data from a source and preparing it for further processing. It utilizes metadata information to define the source and target names and applies a table iterator to process the data. Running the pipeline via Python in a Spark environment ensures efficient and scalable data ingestion.




## Datasets

1. **target_table**
The dataset schema consists of a target table, but no further information is provided about the structure or content of the table.

2. **source_path**
A list of source paths, serving as a reference for the location of various data sources.

3. **MetadataTable**
Metadata table containing information about the source path, target table, and timestamp of data.