# Building an ETL data pipeline with Apache Airflow and Visualizing AWS Redshift Data using Microsoft Power BI

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

<div align="justify">

This project requires that you have prior knowledge of these technologies, however my YouTube video could help you in case you do not have experience with the tools, in this way you can mount the project without the need of previous experience. I will not delve into explaining what Apache Airflow is, this section will focus on explaining the process of data integration of the UBER receipts until reaching a common data source, the final data source is the data model that was designed in the previous section. :running: :stuck_out_tongue_winking_eye:

The DAG is made up of several important tasks, but I will only explain a brief summary  of what it does:


- At the beginning, the task called **Start_UBER_Business** is separating the Uber Eats receipts from the Uber rides receipts found in the S3 bucket **uber-tracking-expenses-bucket-s3** in the folder **unprocessed_receipts**, both groups of receipts will be processed in parallel by the tasks **rides_receipts_to_s3_task** and **eats_receipts_to_s3_task**
- The goal of these two tasks **rides_receipts_to_s3_task** and **eats_receipts_to_s3_task** that are running in parallel, is to condense in a single file all processed receipts of each kind eats and rides, the final datasets will be placed in the bucket **airflow-runs-receipts**, under the **/rides** and **/eats** folders as the case may be, the files are:
  - eats_receipts.csv: Contains the information of all the receipts found for UBER Eats.
  - items_eats_receipts.csv: Contains information of all the products involved in the purchase of an order from UBER eats
  - rides_receipts.csv: Contains information on all receipts found for UBER Eats
- Once the two previous processes are finished, the tasks related to create the necessary objects in redshift are executed, dimension tables, fact tables and staging tables.
- Once the tables were created in Redshift, Now the staging tables will be filled. The COPY command is useful for move the .csv files from the S3 bucket to Redshift, There are several benefits of staging data: <a href="https://help.gooddata.com/doc/enterprise/en/data-integration/data-preparation-and-distribution/data-preparation-and-distribution-pipeline/data-pipeline-reference/data-warehouse-reference/how-to-set-up-a-connection-to-data-warehouse/connecting-to-data-warehouse-from-cloudconnect/loading-data-through-cloudconnect-to-data-warehouse/merging-data-using-staging-tables">Merging Data Using Staging Tables</a>
  - staging_rides
  - staging_eats
  - staging_eats_items
- Once all the staging tables were created and filled, now the dimension tables will be filled, as you can see in the file **sql_statements.py** all the necessary dimensions and fact tables for the DWH depend on the information contained in the staging tables. To maintain the consistency of the data, we re filling the dimensions first and then the fact tables, there would be no problem using other way around, because redshift does not validate the foreign keys, <a href="https://www.stitchdata.com/blog/how-redshift-differs-from-postgresql/">this is because redshift is a database which focuses on handling large volumes of data for analytical queries.</a>
- Once the filling of all the DWH tables is finished, we proceed to validate if there are records in them, it is a good practice to maintain a  <a href="https://arun-karunakaran.medium.com/build-quality-into-extract-transform-and-load-process-c02795ddcc93">data quality check</a> section in your ETL process for data integration. In the end, I deleted the staging tables because they are no longer needed.

</div>

Below is the final DAG for this project:

![alt text](https://wittline.github.io/Uber-expenses-tracking/Images/dag.PNG)




## Visualizing AWS Redshift Data using Microsoft Power BI





