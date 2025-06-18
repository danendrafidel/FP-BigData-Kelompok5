#!/usr/bin/env python3
"""
Job Recommendation System using Apache Spark
This script creates a job recommendation model using both collaborative filtering
and content-based filtering approaches.
"""

import os
import sys
from datetime import datetime
import numpy as np
import pandas as pd

def setup_java_environment():
    """Setup Java environment before importing Spark"""
    jdk_path = r"C:\Program Files\Java\jdk-17"
    
    # Set JAVA_HOME if not already set
    if 'JAVA_HOME' not in os.environ and os.path.exists(jdk_path):
        os.environ['JAVA_HOME'] = jdk_path
        print(f"✓ JAVA_HOME set to: {jdk_path}")
    
    # Add Java bin to PATH
    java_bin = os.path.join(jdk_path, "bin")
    current_path = os.environ.get('PATH', '')
    if java_bin not in current_path and os.path.exists(java_bin):
        os.environ['PATH'] = f"{java_bin};{current_path}"
        print(f"✓ Java bin added to PATH: {java_bin}")
    
    # Set Spark environment variables
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
    
    # Remove problematic configurations
    if 'SPARK_CLASSPATH' in os.environ:
        del os.environ['SPARK_CLASSPATH']

# Setup Java environment before importing Spark
setup_java_environment()

# Spark imports (after Java setup)
try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import *
    from pyspark.sql.types import *
    from pyspark.ml.feature import StringIndexer, VectorAssembler, HashingTF, IDF, StandardScaler
    from pyspark.ml.recommendation import ALS
    from pyspark.ml.classification import RandomForestClassifier
    from pyspark.ml.evaluation import RegressionEvaluator, BinaryClassificationEvaluator
    from pyspark.ml import Pipeline
    print("✓ Spark imports successful")
except ImportError as e:
    print(f"✗ Failed to import Spark: {e}")
    print("Please ensure PySpark is installed: pip install pyspark")
    sys.exit(1)

# MinIO client for listing files (optional)
try:
    from minio import Minio
    from minio.error import S3Error
    print("✓ MinIO imports successful")
    MINIO_AVAILABLE = True
except ImportError as e:
    print(f"Warning: MinIO not available: {e}")
    Minio = None
    S3Error = Exception
    MINIO_AVAILABLE = False

class JobRecommendationSystem:
    def __init__(self, app_name="JobRecommendationSystem"):
        """Initialize Spark session with minimal configuration"""
        
        print(f"Initializing {app_name}...")
        print(f"Java Home: {os.environ.get('JAVA_HOME', 'Not Set')}")
        
        try:
            # Create Spark session with minimal configuration (no external packages)
            self.spark = SparkSession.builder \
                .appName(app_name) \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
                .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
                .getOrCreate()
            
            self.spark.sparkContext.setLogLevel("WARN")
            print(f"✓ Spark session created successfully: {app_name}")
            
        except Exception as e:
            print(f"✗ Error creating Spark session: {e}")
            raise e
        
        # Initialize MinIO client only if available
        self.minio_client = None
        if MINIO_AVAILABLE:
            try:
                self.minio_client = Minio(
                    "localhost:9000",
                    access_key="minio_access_key",
                    secret_key="minio_secret_key",
                    secure=False
                )
                # Test connection
                list(self.minio_client.list_buckets())
                print("✓ MinIO client initialized successfully")
            except Exception as e:
                print(f"Warning: MinIO connection failed: {e}")
                print("Will use local file loading instead")
                self.minio_client = None
        
        # Initialize models
        self.als_model = None
        self.content_model = None
        self.feature_pipeline = None
        
    def download_from_minio_to_local(self, bucket_name="jobs", local_dir="./data"):
        """Download files from MinIO to local directory for processing"""
        if not self.minio_client:
            return []
            
        print(f"Downloading files from MinIO bucket '{bucket_name}' to '{local_dir}'...")
        os.makedirs(local_dir, exist_ok=True)
        
        try:
            objects = list(self.minio_client.list_objects(bucket_name, recursive=True))
            csv_files = [obj for obj in objects if obj.object_name.endswith('.csv')]
            
            downloaded_files = []
            for obj in csv_files[:5]:  # Limit to 5 files for testing
                local_file = os.path.join(local_dir, os.path.basename(obj.object_name))
                print(f"Downloading {obj.object_name}...")
                self.minio_client.fget_object(bucket_name, obj.object_name, local_file)
                downloaded_files.append(local_file)
                print(f"  ✓ Downloaded to {local_file}")
            
            return downloaded_files
            
        except Exception as e:
            print(f"Error downloading from MinIO: {e}")
            return []
    
    def load_data_from_local_files(self, file_paths=None, data_dir="./data"):
        """Load data from local CSV files"""
        if file_paths is None:
            # Find CSV files in data directory
            if os.path.exists(data_dir):
                file_paths = [os.path.join(data_dir, f) for f in os.listdir(data_dir) if f.endswith('.csv')]
            else:
                # Look for CSV files in current directory
                file_paths = [f for f in os.listdir('.') if f.endswith('.csv')]
        
        if not file_paths:
            raise Exception("No CSV files found to load")
        
        print(f"Loading data from {len(file_paths)} CSV files...")
        
        dataframes = []
        total_records = 0
        
        for file_path in file_paths:
            if not os.path.exists(file_path):
                print(f"File not found: {file_path}")
                continue
                
            try:
                print(f"Loading: {file_path}")
                df_batch = self.spark.read.option("header", "true") \
                                         .option("inferSchema", "true") \
                                         .option("multiline", "true") \
                                         .option("escape", '"') \
                                         .csv(file_path)
                
                batch_count = df_batch.count()
                total_records += batch_count
                print(f"  - Loaded {batch_count} records")
                
                # Add file identifier
                df_batch = df_batch.withColumn("source_file", lit(os.path.basename(file_path)))
                dataframes.append(df_batch)
                
            except Exception as e:
                print(f"Error loading {file_path}: {str(e)}")
                continue
        
        if not dataframes:
            raise Exception("No data could be loaded from any files")
        
        # Union all dataframes
        print("Combining all datasets...")
        df_combined = dataframes[0]
        for df in dataframes[1:]:
            df_combined = df_combined.unionByName(df, allowMissingColumns=True)
        
        print(f"Combined dataset: {total_records} records from {len(dataframes)} files")
        
        # Clean and preprocess data
        df_cleaned = self.clean_data(df_combined)
        return df_cleaned
    
    def load_data(self):
        """Load data from available sources (MinIO first, then local)"""
        print("Loading data...")
        
        # Try MinIO first
        if self.minio_client:
            try:
                downloaded_files = self.download_from_minio_to_local()
                if downloaded_files:
                    return self.load_data_from_local_files(downloaded_files)
            except Exception as e:
                print(f"MinIO loading failed: {e}")
        
        # Fallback to local files
        print("Loading from local files...")
        return self.load_data_from_local_files()
    
    def clean_data(self, df):
        """Clean and preprocess the dataset"""
        print("Cleaning and preprocessing data...")
        
        # Show schema and sample data
        print("Dataset schema:")
        df.printSchema()
        print("\nSample data:")
        df.show(3, truncate=False)
        
        # Get available columns
        available_cols = df.columns
        print(f"Available columns: {available_cols}")
        
        # Remove records with null values in first few important columns
        important_cols = available_cols[:4]  # Use first 4 columns as important
        filter_condition = col(important_cols[0]).isNotNull()
        for col_name in important_cols[1:]:
            filter_condition = filter_condition & col(col_name).isNotNull()
        
        df_cleaned = df.filter(filter_condition)
        
        # Create basic features
        df_cleaned = df_cleaned.withColumn("record_id", monotonically_increasing_id())
        df_cleaned = df_cleaned.withColumn("user_id", (col("record_id") % 1000).cast("int"))
        df_cleaned = df_cleaned.withColumn("item_id", (col("record_id") % 5000).cast("int"))
        
        # Create a rating based on available data (example logic)
        df_cleaned = df_cleaned.withColumn("rating", 
            when(col("record_id") % 5 == 0, 5.0)
            .when(col("record_id") % 4 == 0, 4.0)
            .when(col("record_id") % 3 == 0, 3.0)
            .when(col("record_id") % 2 == 0, 2.0)
            .otherwise(1.0)
        )
        
        # Index categorical columns if they exist
        categorical_cols = []
        for col_name in available_cols:
            if df_cleaned.schema[col_name].dataType == StringType():
                categorical_cols.append(col_name)
        
        # Index up to 3 categorical columns
        indexers = []
        for i, col_name in enumerate(categorical_cols[:3]):
            output_col = f"cat_feature_{i}"
            indexer = StringIndexer(inputCol=col_name, outputCol=output_col, handleInvalid="keep")
            indexers.append(indexer)
        
        if indexers:
            pipeline_indexer = Pipeline(stages=indexers)
            df_indexed = pipeline_indexer.fit(df_cleaned).transform(df_cleaned)
        else:
            df_indexed = df_cleaned
        
        print(f"Data cleaned. Final dataset has {df_indexed.count()} records")
        return df_indexed
    
    def train_collaborative_filtering(self, df):
        """Train ALS collaborative filtering model"""
        print("Training collaborative filtering model (ALS)...")
        
        # Prepare data for ALS
        als_data = df.select("user_id", "item_id", "rating")
        
        # Split data
        (training, test) = als_data.randomSplit([0.8, 0.2], seed=42)
        
        print(f"Training set: {training.count()} records")
        print(f"Test set: {test.count()} records")
        
        # ALS model with reduced parameters for faster training
        als = ALS(
            maxIter=5,
            regParam=0.1,
            userCol="user_id",
            itemCol="item_id",
            ratingCol="rating",
            coldStartStrategy="drop",
            nonnegative=True
        )
        
        # Train model
        print("Training ALS model...")
        self.als_model = als.fit(training)
        print("✓ ALS model trained successfully")
        
        # Evaluate model
        predictions = self.als_model.transform(test)
        evaluator = RegressionEvaluator(
            metricName="rmse",
            labelCol="rating",
            predictionCol="prediction"
        )
        rmse = evaluator.evaluate(predictions)
        print(f"RMSE on test data: {rmse:.4f}")
        
        return training, test
    
    def train_content_based_filtering(self, df):
        """Train content-based filtering model using available features"""
        print("Training content-based filtering model...")
        
        # Create features from available columns
        feature_cols = []
        
        # Use categorical features if available
        cat_features = [col for col in df.columns if col.startswith("cat_feature_")]
        feature_cols.extend(cat_features)
        
        # Add user_id and item_id as features
        feature_cols.extend(["user_id", "item_id"])
        
        if not feature_cols:
            print("No features available for content-based filtering")
            return None, None
        
        print(f"Using features: {feature_cols}")
        
        # Create binary classification problem (high rating vs low rating)
        df_binary = df.withColumn(
            "high_rating",
            when(col("rating") >= 4.0, 1.0).otherwise(0.0)
        )
        
        # Assemble features
        assembler = VectorAssembler(
            inputCols=feature_cols,
            outputCol="features"
        )
        
        df_features = assembler.transform(df_binary)
        
        # Split data
        (training, test) = df_features.randomSplit([0.8, 0.2], seed=42)
        
        # Random Forest classifier
        rf = RandomForestClassifier(
            featuresCol="features",
            labelCol="high_rating",
            numTrees=10,
            maxDepth=5,
            seed=42
        )
        
        # Train model
        print("Training Random Forest model...")
        self.content_model = rf.fit(training)
        print("✓ Content-based model trained successfully")
        
        # Evaluate
        predictions = self.content_model.transform(test)
        evaluator = BinaryClassificationEvaluator(
            labelCol="high_rating",
            rawPredictionCol="rawPrediction"
        )
        auc = evaluator.evaluate(predictions)
        print(f"AUC for content-based model: {auc:.4f}")
        
        return training, test
    
    def generate_recommendations(self, user_id, num_recommendations=10):
        """Generate recommendations for a specific user"""
        if self.als_model is None:
            print("Error: ALS model not trained yet!")
            return None
        
        print(f"Generating recommendations for user {user_id}...")
        
        # Generate recommendations using ALS
        user_df = self.spark.createDataFrame([(user_id,)], ["user_id"])
        recommendations = self.als_model.recommendForUserSubset(user_df, num_recommendations)
        
        return recommendations
    
    def save_models(self, model_path="./models"):
        """Save trained models to disk"""
        print(f"Saving models to {model_path}...")
        
        # Create models directory
        os.makedirs(model_path, exist_ok=True)
        
        # Save ALS model
        if self.als_model:
            als_path = os.path.join(model_path, "als_model")
            self.als_model.write().overwrite().save(als_path)
            print(f"✓ ALS model saved to {als_path}")
        
        # Save content-based model
        if self.content_model:
            content_path = os.path.join(model_path, "content_model")
            self.content_model.write().overwrite().save(content_path)
            print(f"✓ Content-based model saved to {content_path}")
        
        print("Models saved successfully!")
    
    def close(self):
        """Close Spark session"""
        self.spark.stop()
        print("Spark session closed")

def main():
    """Main function to run the job recommendation system"""
    print("=" * 60)
    print("Job Recommendation System - Training Pipeline")
    print("=" * 60)
    
    job_rec_system = None
    
    try:
        # Initialize system
        job_rec_system = JobRecommendationSystem()
        
        # Load data
        df = job_rec_system.load_data()
        
        # Show basic statistics
        print(f"\nDataset Statistics:")
        print(f"Total records: {df.count()}")
        print(f"Columns: {len(df.columns)}")
        
        # Train collaborative filtering model
        print("\n" + "="*40)
        print("Training Collaborative Filtering Model")
        print("="*40)
        train_cf, test_cf = job_rec_system.train_collaborative_filtering(df)
        
        # Train content-based filtering model
        print("\n" + "="*40)
        print("Training Content-Based Filtering Model")
        print("="*40)
        train_cb, test_cb = job_rec_system.train_content_based_filtering(df)
        
        # Generate sample recommendations
        print("\n" + "="*40)
        print("Generating Sample Recommendations")
        print("="*40)
        sample_user_id = 1
        recommendations = job_rec_system.generate_recommendations(sample_user_id, 5)
        
        if recommendations:
            print(f"Top 5 recommendations for user {sample_user_id}:")
            recommendations.show(truncate=False)
        
        # Save models
        print("\n" + "="*40)
        print("Saving Models")
        print("="*40)
        job_rec_system.save_models("./models")
        
        print("\n" + "=" * 60)
        print("✅ Training completed successfully!")
        print("Models saved to: ./models")
        print("=" * 60)
        
    except Exception as e:
        print(f"❌ Error during execution: {str(e)}")
        import traceback
        traceback.print_exc()
    
    finally:
        # Clean up
        if job_rec_system:
            job_rec_system.close()

if __name__ == "__main__":
    main()