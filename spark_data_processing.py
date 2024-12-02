from pyspark.sql import SparkSession
from pyspark.sql.functions import when
from pyspark.sql.types import NumericType

spark = SparkSession.builder \
    .appName("Load CSV Files and Process Data") \
    .getOrCreate()

# File paths
ddinter_path = "data/ddinter_downloads_code_A.csv"
drug_cids_path = "data/drug_cids.csv"
pubchem_path = "data/pubchem_data.csv"

# Load DataFrames
ddinter_df = spark.read.csv(ddinter_path, header=True, inferSchema=True)
drug_cids_df = spark.read.csv(drug_cids_path, header=True, inferSchema=True)
pubchem_df = spark.read.csv(pubchem_path, header=True, inferSchema=True)

# Map 'Level' column from factors to integers
ddinter_df = ddinter_df.withColumn(
    "Level",
    when(ddinter_df["Level"] == "Minor", 0)
    .when(ddinter_df["Level"] == "Moderate", 1)
    .when(ddinter_df["Level"] == "Major", 2)
    .otherwise(None)
)

# Identify numerical columns in pubchem_df
numerical_cols = [field.name for field in pubchem_df.schema.fields if isinstance(field.dataType, NumericType)]

# Select only numerical columns
pubchem_numeric_df = pubchem_df.select(*numerical_cols)

# Rename 'CIDs' to 'cid' in drug_cids_df
drug_cids_df = drug_cids_df.withColumnRenamed("CIDs", "cid")

# Perform an inner join
joined_df = drug_cids_df.join(pubchem_numeric_df, on="cid", how="inner")

# Perform join between ddinter_df and joined_df on 'Drug_A' and 'Drug Name'
ddinter_drugA_joined = ddinter_df.join(joined_df, ddinter_df["Drug_A"] == joined_df["Drug Name"], how="inner")

# Perform join between ddinter_drugA_joined and joined_df on 'Drug_B' and 'Drug Name'
ddinter_drugA_drugB_joined = ddinter_drugA_joined.join(
    joined_df, 
    ddinter_drugA_joined["Drug_B"] == joined_df["Drug Name"], 
    how="inner"
)

# Save the final DataFrame as a CSV file in the 'data' folder
output_path = "data/final_joined_data.csv"
ddinter_drugA_drugB_joined.write.csv(output_path, header=True, mode="overwrite")

# Stop Spark session
spark.stop()
