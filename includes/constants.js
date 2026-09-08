// Reads the 'env' variable passed from Airflow vars ('dev' or 'prod')
const env = dataform.projectConfig.vars.env || "dev";

// Map environment explicitly to exact schema targets
const TARGET_SCHEMA = env === "dev" ? "etl_training_bq_dev" : "etl_training_bq";

module.exports = { TARGET_SCHEMA };