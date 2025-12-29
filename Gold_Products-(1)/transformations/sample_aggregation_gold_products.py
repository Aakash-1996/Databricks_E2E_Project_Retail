@dlt.table(name = "DimProducts_stg")
def DimProducts_stg():
  return spark.readStream.table("master_catalog.silver.products")

@dlt.view(name = "DimProducts_view")
def DimProducts_view():
  return spark.readStream.table("DimProducts_stg")

dlt.create_streaming_table(
  name = "DimProducts1",
  comment = "This is the final table for DimProducts")

dlt.apply_changes(
  target = "DimProducts1",
  source = "DimProducts_view",
  keys = ["product_id"],
  sequence_by = "Received_ts",
  stored_as_scd_type = 1)

dlt.create_streaming_table(
  name = "DimProducts2",
  comment = "This is the final table for DimProducts")

dlt.apply_changes(
  target = "DimProducts2",
  source = "DimProducts_view",
  keys = ["product_id"],
  sequence_by = "Received_ts",
  stored_as_scd_type = 2)


