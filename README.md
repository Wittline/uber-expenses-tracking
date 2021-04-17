# Building an ETL data pipeline with Apache Airflow and Visualizing Redshift Data using Power BI

<div align="justify">


Have you heard phrases like ***Hungry? You're in the right place*** or ***Request a trip, hop in, and relax.*** ? :roll_eyes: Both phrases are very common in our daily lives, they represent the emblems of the two most important businesses with <a href="https://qz.com/1889602/uber-q2-2020-earnings-eats-is-now-bigger-than-rides/"> millionaire revenues </a> from UBER. ***Have you ever thought about how much money you spend on these services?*** The goal of this project is to track the expenses of <a href="https://www.uber.com/">Uber Rides</a> and <a  href="https://www.ubereats.com/">Uber Eats</a> through a Data Engineering processes using technologies such as <a href="https://airflow.apache.org/">Apache Airflow</a>, <a href="https://aws.amazon.com/es/redshift/">AWS Redshift</a> and <a href="https://powerbi.microsoft.com/es-es/">Power BI</a>. Keep reading this article, I will show you a quick and easy way to automate everything step by step.


</div>

## What are the data sources?

<div align="justify">
 
Every time an Ubers Eat or Uber Rides service has ended, you will receive a payment receipt to your email, this receipt contains the information about the details of the service, and is attached to the email with the extension ***.eml***. Take a look at the image below, both receipts belong to the details sent by Uber about Eats and Rides services, this will be our original data sources, In my case, I downloaded all those receipts from my email to my local computer.

</div>

> Uber Rides receipt example
> 
![alt text](https://wittline.github.io/Uber-expenses-tracking/Images/rides_receipt_example.PNG)

> Uber Eats receipt example
> 
![alt text](https://wittline.github.io/Uber-expenses-tracking/Images/eats_receipt_example.PNG)

## Data modelling

<div align="justify">
 
Once the details for each type of receipt have been detected, it is easy to know what are the features, entities, and relations of the model. My proposed data model contains the expenses of both services separated in different fact tables, sharing dimensions between these facts, therefore, the proposed model will be a constellation scheme.

</div>

![alt text](https://wittline.github.io/Uber-expenses-tracking/Images/dwh_schema.jpg)

## Infrastructure as Code (IaC) in AWS

<div align="justify">
 
The aim of this section is to create a Redshift cluster on AWS and keep it available for use by the airflow DAG. In addition to preparing the infrastructure, the file ***AWS-IAC-IAM-EC2-S3-Redshift.ipynb*** will help you to have an alternative staging zone in S3 as well.

Below we list the different steps that are carried out in this file:

- Create the required S3 buckets
  - uber-tracking-expenses-bucket-s3
  - airflow-runs-receipts
- Move Uber receipts from your local computer to S3
- Loading params from the dwh.cfg file
- Creating clients for IAM, EC2 and Redshift cluster
- Creating the IAM Role that makes Redshift able to access to S3 buckets **ReadOnly**
- Creating Redshift cluster
- Check cluster details until status show Available
- Showing Redshift cluster endpoint and role ARN
- Incoming TCP port to access to the cluster ednpoint
- Checking the connection to the Redshift cluster
- Cleaning and deleting the resources

 ```python
put code here
 ```

</div>

## Building an ETL data pipeline with Apache Airflow


## Visualizing AWS Redshift Data using Microsoft Power BI





