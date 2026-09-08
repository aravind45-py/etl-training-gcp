const env = dataform.projectConfig.vars.env || "prod";

// Map environment explicitly to exact schema targets
const targetSchema = {
  dev: "etl_training_bq_dev",
  prod: "etl_training_bq_prod"
}[env];

module.exports = { targetSchema };