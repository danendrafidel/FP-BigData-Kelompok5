#!/usr/bin/env python3
"""
Job Recommendation System - Usage Example
This script demonstrates how to use the trained job recommendation models
"""

import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Import our recommendation system
from spark import JobRecommendationSystem

class JobRecommendationDemo:
    def __init__(self):
        """Initialize the recommendation demo"""
        self.rec_system = JobRecommendationSystem("JobRecommendationDemo")
        
    def load_pretrained_models(self, model_path="./models"):
        """Load pre-trained models"""
        print("Loading pre-trained models...")
        self.rec_system.load_models(model_path)
        
    def recommend_jobs_for_user(self, user_id, num_recommendations=10):
        """Get job recommendations for a specific user"""
        print(f"\nGenerating {num_recommendations} job recommendations for user {user_id}...")
        
        recommendations = self.rec_system.generate_recommendations(user_id, num_recommendations)
        
        if recommendations:
            print("Recommendations:")
            recommendations.show(truncate=False)
            return recommendations
        else:
            print("No recommendations available. Please train the model first.")
            return None
    
    def find_similar_jobs(self, job_id, dataset_path="./kafka/dataset.csv", num_similar=5):
        """Find jobs similar to a given job"""
        print(f"\nFinding {num_similar} jobs similar to job {job_id}...")
        
        # Load dataset
        df = self.rec_system.load_data(dataset_path)
        
        # Prepare features
        df_features, _ = self.rec_system.prepare_features_for_content_based(df)
        
        # Get similar jobs (simplified approach)
        target_job = df_features.filter(col("Job Id") == job_id)
        if target_job.count() > 0:
            similar_jobs = self.rec_system.get_similar_jobs(None, df_features, num_similar)
            print("Similar jobs:")
            similar_jobs.show(truncate=False)
            return similar_jobs
        else:
            print(f"Job with ID {job_id} not found.")
            return None
    
    def get_job_insights(self, dataset_path="./kafka/dataset.csv"):
        """Get insights about the job dataset"""
        print("\nGenerating job market insights...")
        
        df = self.rec_system.load_data(dataset_path)
        
        print("\n1. Top 10 Countries by Job Count:")
        df.groupBy("Country").count().orderBy(desc("count")).show(10)
        
        print("\n2. Top 10 Job Roles:")
        df.groupBy("Role").count().orderBy(desc("count")).show(10)
        
        print("\n3. Work Type Distribution:")
        df.groupBy("Work Type").count().orderBy(desc("count")).show()
        
        print("\n4. Average Salary by Country (Top 10):")
        df.filter(col("avg_salary").isNotNull()) \
          .groupBy("Country") \
          .agg(avg("avg_salary").alias("avg_salary"), count("*").alias("job_count")) \
          .filter(col("job_count") >= 10) \
          .orderBy(desc("avg_salary")) \
          .show(10)
        
        print("\n5. Experience Level Distribution:")
        df.groupBy("experience_years").count().orderBy("experience_years").show()
        
    def search_jobs_by_criteria(self, dataset_path="./kafka/dataset.csv", 
                               country=None, role=None, min_salary=None, 
                               work_type=None, skills=None):
        """Search jobs based on specific criteria"""
        print("\nSearching jobs based on criteria...")
        
        df = self.rec_system.load_data(dataset_path)
        
        # Apply filters
        filtered_df = df
        
        if country:
            filtered_df = filtered_df.filter(lower(col("Country")).contains(country.lower()))
        
        if role:
            filtered_df = filtered_df.filter(lower(col("Role")).contains(role.lower()))
        
        if min_salary:
            filtered_df = filtered_df.filter(col("avg_salary") >= min_salary)
        
        if work_type:
            filtered_df = filtered_df.filter(lower(col("Work Type")).contains(work_type.lower()))
        
        if skills:
            filtered_df = filtered_df.filter(lower(col("skills")).contains(skills.lower()))
        
        print(f"Found {filtered_df.count()} jobs matching criteria:")
        filtered_df.select("Job Title", "Company", "Country", "Role", "Work Type", 
                          "avg_salary", "skills").show(20, truncate=False)
        
        return filtered_df
    
    def train_new_model(self, dataset_path="./kafka/dataset.csv", model_path="./models"):
        """Train a new recommendation model"""
        print("Training new recommendation model...")
        
        # Load and preprocess data
        df = self.rec_system.load_data(dataset_path)
        
        # Prepare features
        df_features, feature_model = self.rec_system.prepare_features_for_content_based(df)
        
        # Train models
        train_cf, test_cf = self.rec_system.train_collaborative_filtering(df)
        train_cb, test_cb = self.rec_system.train_content_based_filtering(df_features)
        
        # Save models
        self.rec_system.save_models(model_path)
        
        print("Model training completed!")
    
    def close(self):
        """Close the recommendation system"""
        self.rec_system.close()

def interactive_demo():
    """Run an interactive demo of the job recommendation system"""
    demo = JobRecommendationDemo()
    
    try:
        print("=" * 70)
        print("Job Recommendation System - Interactive Demo")
        print("=" * 70)
        
        while True:
            print("\nAvailable options:")
            print("1. Train new model")
            print("2. Load existing model and get recommendations")
            print("3. Find similar jobs")
            print("4. Get job market insights")
            print("5. Search jobs by criteria")
            print("6. Exit")
            
            choice = input("\nEnter your choice (1-6): ").strip()
            
            if choice == "1":
                demo.train_new_model()
                
            elif choice == "2":
                try:
                    demo.load_pretrained_models()
                    user_id = int(input("Enter user ID (0-9999): "))
                    num_recs = int(input("Number of recommendations (default 10): ") or "10")
                    demo.recommend_jobs_for_user(user_id, num_recs)
                except ValueError:
                    print("Invalid input. Please enter valid numbers.")
                except Exception as e:
                    print(f"Error: {e}")
                    print("Please train the model first using option 1.")
                    
            elif choice == "3":
                job_id = input("Enter job ID: ").strip()
                num_similar = int(input("Number of similar jobs (default 5): ") or "5")
                demo.find_similar_jobs(job_id, num_similar=num_similar)
                
            elif choice == "4":
                demo.get_job_insights()
                
            elif choice == "5":
                print("\nEnter search criteria (leave empty to skip):")
                country = input("Country: ").strip() or None
                role = input("Role: ").strip() or None
                work_type = input("Work Type: ").strip() or None
                skills = input("Skills: ").strip() or None
                min_salary_input = input("Minimum Salary: ").strip()
                min_salary = int(min_salary_input) if min_salary_input else None
                
                demo.search_jobs_by_criteria(
                    country=country, role=role, min_salary=min_salary,
                    work_type=work_type, skills=skills
                )
                
            elif choice == "6":
                print("Thank you for using the Job Recommendation System!")
                break
                
            else:
                print("Invalid choice. Please enter a number between 1 and 6.")
                
    except KeyboardInterrupt:
        print("\nExiting...")
    
    finally:
        demo.close()

def quick_demo():
    """Run a quick demonstration of the system capabilities"""
    demo = JobRecommendationDemo()
    
    try:
        print("=" * 70)
        print("Job Recommendation System - Quick Demo")
        print("=" * 70)
        
        # Get job insights
        demo.get_job_insights()
        
        # Search for Python developer jobs
        print("\n" + "=" * 50)
        print("Example: Searching for Python Developer jobs in USA")
        print("=" * 50)
        demo.search_jobs_by_criteria(
            country="United States", 
            role="Developer", 
            skills="Python",
            min_salary=50000
        )
        
        print("\nTo train models and get personalized recommendations, run the interactive demo.")
        
    finally:
        demo.close()

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--interactive":
        interactive_demo()
    else:
        quick_demo()
