from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression

# Initialize Spark session
spark = SparkSession.builder.appName("MLPipeline").getOrCreate()

# Load data
df = spark.read.csv("s3://your-bucket/data.csv", header=True, inferSchema=True)

# Define pipeline stages
assembler = VectorAssembler(inputCols=["feature1", "feature2"], outputCol="features")
lr = LogisticRegression(featuresCol="features", labelCol="label")

# Build and run pipeline
pipeline = Pipeline(stages=[assembler, lr])
model = pipeline.fit(df)
model.save("s3://your-bucket/model")

# Schedule as Databricks Job via CLI: 
# databricks jobs create --json '{"name": "Daily Training", "existing_cluster_id": "your-cluster", "spark_jar_task": {"main_class_name": "com.example.Main"}}'
