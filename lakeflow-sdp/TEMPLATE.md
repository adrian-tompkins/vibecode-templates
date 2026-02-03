# Lakeflow SDP Template

This template provides a starting point for Databricks Spark Declarative Pipeline (SDP) projects.

## What This Template Provides

- **Databricks Asset Bundle (DAB)** configuration for deployment
- **Spark Declarative Pipeline (SDP)** structure with medallion architecture
- **Standard project layout** for organizing pipeline code

## Using This Template

To bootstrap a new project from this template:

1. Copy this directory to your new project location
2. Update `databricks.yml` with your project name, catalog, and schema
3. Modify the sample pipeline in `src/pipelines/basic_pipeline.py`
4. Initialize git: `git init && git add . && git commit -m "Initial commit from lakeflow-sdp template"`

## Project Structure

```
.
├── databricks.yml           # Databricks Asset Bundle configuration
└── src/
    └── pipelines/           # SDP pipeline definitions
        ├── basic_pipeline.py              # Simple medallion architecture
        ├── advanced_flows_cdc_pipeline.py # Flows and CDC patterns
        └── kafka_pipeline.py              # Kafka ingestion examples
```

## Key Concepts

### Spark Declarative Pipelines (SDP)
SDP uses declarative syntax with `@dp.table` decorators to define data transformations. Import with:
```python
from pyspark import pipelines as dp
```

### Databricks Asset Bundles (DABs)
DABs enable CI/CD deployment of Databricks resources including pipelines, jobs, and notebooks.

### Medallion Architecture
The example pipeline demonstrates the medallion architecture:
- **Bronze**: Raw data ingestion
- **Silver**: Cleaned and validated data
- **Gold**: Business-level aggregates

## Next Steps

After bootstrapping:
1. Configure your Databricks workspace connection
2. Deploy with: `databricks bundle deploy -t dev`
3. Run your pipeline from the Databricks workspace UI
4. Set up CI/CD for automated deployments
