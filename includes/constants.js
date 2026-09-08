// Reads the schema_suffix passed from Airflow vars, defaulting to empty string
const suffix = dataform.projectConfig.vars.schema_suffix || "";

// Dynamically sets the target dataset name
const TARGET_SCHEMA = `etl_training_bq${suffix}`;

module.exports = { TARGET_SCHEMA };