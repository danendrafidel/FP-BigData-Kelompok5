#!/bin/sh
curl -L -o dataset.zip https://www.kaggle.com/api/v1/datasets/download/ravindrasinghrana/job-description-dataset
unzip dataset.zip -d kafka
mv kafka/job_descriptions.csv kafka/dataset.csv
rm dataset.zip
echo "Dataset ready for use!"