# DevDay Lab: Serverless ETL and Data Discovery using AWS Glue and Amazon Athena

* [Create an IAM Role](#create-an-iam-role)
* [Create an Amazon S3 bucket](#create-an-amazon-s3-bucket)
* [Discover the Data](#discover-the-data)
* [Optimize the Queries and convert into Parquet](#optimize-the-queries-and-convert-into-parquet)
* [Query the Partitioned Data using Amazon Athena](#query-the-partitioned-data-using-amazon-athena)
* [Deleting the Glue database, crawlers and ETL Jobs created for this Lab](#deleting-the-glue-database-crawlers-and-etl-jobs-created-for-this-lab)
* [Summary](#summary)

## Architectural Diagram
![architecture-overview-lab3.png](https://s3-us-west-2.amazonaws.com/reinvent2017content-abd313/lab3/Screen+Shot+2017-11-17+at+1.11.32+AM.png)

## Create an IAM Role

Create an IAM role that has permission to your Amazon S3 sources, targets, temporary directory, scripts, **AWSGlueServiceRole** and any libraries used by the job. You can click [here](https://console.aws.amazon.com/iam/home?region=us-west-2#/roles) to create a new role. For additional documentation to create a role [here](docs.aws.amazon.com/cli/latest/reference/iam/create-role.html).

1. On the IAM Page, click on **Create Role**.
2. Choose the service as **Glue** and click on **Next: Permissions** on the bottom.
3. On the Attach permissions policies, search policies for S3 and check the box for **AmazonS3FullAccess**.

> Do not click on the policy, you just have to check the corresponding checkbox.

4. On the same page, now search policies for Glue and check the box for **AWSGlueServiceRole** and **AWSGlueConsoleFullAccess**.

> Do not click on the policy, you just have to check the corresponding checkbox.

5. Click on **Next: Review**.
6. Enter Role name as

```
nycitytaxi-meldevday
```

​	and click Finish.

## Create an Amazon S3 bucket

1. Open the [AWS Management console for Amazon S3](https://s3.console.aws.amazon.com/s3/home?region=us-west-2)
2. On the S3 Dashboard, Click on **Create Bucket**.

![createbucket.png](https://s3-us-west-2.amazonaws.com/reinvent2017content-abd313/lab1/createbucket.png)

1. In the **Create Bucket** pop-up page, input a unique **Bucket name**. So it’s advised to choose a large bucket name, with many random characters and numbers (no spaces). It will be easier to name your bucket

   ```
   aws-glue-scripts-<YOURAWSACCOUNTID>-us-west-2
   ```

   and it would be easier to choose/select this bucket for the remainder of this Lab3.

   i.Select the region as **Oregon**.
   ii. Click **Next** to navigate to next tab.
   iii. In the **Set properties** tab, leave all options as default.
   iv. In the **Set permissions** tag, leave all options as default.
   v. In the **Review** tab, click on **Create Bucket**

![createbucketpopup.png](https://s3-us-west-2.amazonaws.com/reinvent2017content-abd313/lab1/createbucketpopup.png)

2. Now, in this newly created bucket, create two sub-buckets **tmp** and **target** using the same instructions as the above step. We will use these buckets as part of Lab3 later on.

## Discover the Data

During this workshop, we will focus on Q417 of the New York City Taxi Records dataset, however you could easily do this for the entire eight years of data. As you crawl this unknown dataset, you discover that the data is in different formats, depending on the type of taxi. You then convert the data to a canonical form, start to analyze it, and build a set of visualizations. All without launching a single server.

> For this lab, you choose the **Asia Pacific (Sydney)** region.

1. Open the [AWS Management console for Amazon Glue](https://ap-southeast-2.console.aws.amazon.com/glue/home?region=ap-southeast-2#).

2. To analyze all the taxi rides for Q4 2018, you start with a set of data in S3. First, create a database for this workshop within AWS Glue. A database is a set of associated table definitions, organized into a logical group. In Glue Catalog, database names are all lowercase, no matter what you type.

   i. Click on **Databases** under Data Catalog column on the left.

   ![glue1](https://s3-us-west-2.amazonaws.com/reinvent2017content-abd313/lab3/glue_1.PNG)

   ii. Click on the **Add Database** button.

   iii. Enter the Database name as **nycitytaxi-devday18**. You can skip the description and location fields and click on **Create**.

3. Click on **Crawlers** under Data Catalog column on the left.

   ![glue2](https://s3-us-west-2.amazonaws.com/reinvent2017content-abd313/lab3/glue_2.PNG)

   i. Click on **Add Crawler** button.

   ii. Under Add information about your crawler, for Crawler name type **nycitytaxi-crawler-devday18**. You can skip the Description and Classifiers field and click on **Next**.

   iii. Under Data Store, choose S3. And Ensure the radio button for **Crawl Data in Specified path** is checked.

   iv. For Include path, enter the following S3 path and click on **Next**.

   ```
   s3://serverless-analytics/glue-blog/yellow
   ```

   v. For Add Another data store, choose **Yes** and click on **Next**.

   vi. Enter path for the Green Taxi data:

   ```
   s3://serverless-analytics/glue-blog/green
   ```

   vii. For Choose an IAM Role, select **Create an IAM role** and enter the role name as following and click on **Next**.

   ```
   nycitytaxi-devday18-crawler
   ```

   vii. For Create a schedule for this crawler, choose Frequency as **Run on Demand** and click on **Next**.

   viii. Configure the crawler output database and prefix:

   ​	a. For **Database**, select the database created earlier, **nycitytaxi-devday18**.

   ​	b. For **Prefix added to tables (optional)**, type **devday18_** and click on **Next**.

   ​	c. Review configuration and click on **Finish** and on the next page, click on **Run it now** in the green box on the top.

   ![glue14](https://s3-us-west-2.amazonaws.com/reinvent2017content-abd313/lab3/glue_14.PNG)

   ​	d. The crawler runs and indicates that it found two tables.

4. Click on **Tables**, under Data Catalog on the left column.

5. If you look under **Tables**, you can see the three new tables that were created under the database nycitytaxi-devday18.

   ![glue4](https://s3-us-west-2.amazonaws.com/reinvent2017content-abd313/lab3/glue_4.PNG)

6. The crawler used the built-in classifiers and identified the tables as CSV, inferred the columns/data types, and collected a set of properties for each table. If you look in each of those table definitions, you see the number of rows for each dataset found and that the columns don’t match between tables. As an example, clicking on the devday18_yellow table, you can see the yellow dataset for Q417 with 28.5 million records, the location on S3, and the various columns found.

   ![glue5](https://s3-us-west-2.amazonaws.com/reinvent2017content-abd313/lab3/glue_5.PNG)

7. You can run queries against CSV format data however specially for aggregate type queries (e.g. show me average distance of all rides) since CSV is a row based format cost of running the such queries is high and performance is not optimal. Try following query in Amazon Athena and notice the 'Data scanned:' after the query is finished.

   ```
   select count(*) from reinv17_yellow;
   ```

   i. Open the [AWS Management console for Amazon Athena](https://ap-southeast-2.console.aws.amazon.com/athena/home?force&region=ap-southeast-2).

   x. On the left pane for Database select "nycitytaxi-devday18", copy paste the above SQL statement to the right New query space and "Run query".



## Transform CSV into Parquet, Combine the tables and Optimize the Queries

Create an ETL job to transform this data into a query-optimized form. You convert the data into a column format, changing the storage type to Parquet, and writing the data to a bucket in your account. You are also combining the Green and Yellow data sets.

1. Open the [AWS Management console for Amazon Glue](https://ap-southeast-2.console.aws.amazon.com/glue/home?region=ap-southeast-2).

2. Click on **Jobs** under ETL on the left column and then click on the **Add Job** button.

3. Under Job properties, input name as **nycitytaxi-devday18-combined**.

   i. Under  IAM Role, Choose the IAM role created at the beginning of this lab.

   x. Under This job runs, choose the radio button for **A proposed script generated by AWS Glue**.

   xi. For Script file name, enter **nycitytaxi-devday18-combined**.

   ![glue15](https://s3-us-west-2.amazonaws.com/reinvent2017content-abd313/lab3/glue_15.PNG)

   xii. Click on Advanced properties, and select **Enable** for Job bookmark.

   xiii. Here's a screenshot of a finished job properties window:

   ![glue16](https://s3-us-west-2.amazonaws.com/reinvent2017content-abd313/lab3/glue_16.PNG)

4. Click **Next**.

5. Under Choose your data sources, select **devday18_yellow** table as the data source and click on **Next**.

   > For this workshop, we are only working on the yellow dataset. Feel free to run through these steps to also convert the green and FHV dataset.

6. Under Choose your data targets, select the radio button for **Create tables in your data target**.

   i. For Data store, Choose **Amazon S3**.

   ii. For Format, choose **Parquet**.

   iii. For Target path, **click on the folder icon** and choose the target folder previously created. **This S3 Bucket/Folder will contain the transformed Parquet data**.

![glue17](https://s3-us-west-2.amazonaws.com/reinvent2017content-abd313/lab3/glue_17.PNG)

7. Under Map the source columns to target columns page,

   i. Under Target, change the Column name **tpep_pickup_datetime** to **pickup_date**. Click on its respective **data type** field string and change the Column type to **TIMESTAMP** and click on **Update**.

   ii. Under Target, change the Column name **tpep_dropoff_datetime** to **dropoff_date**. Click on its respective **data type** field string and change the Column type to **TIMESTAMP** and click on **Update**.

   iii. Choose **Next**, verify the information and click **Finish**.

![glue9](https://s3-us-west-2.amazonaws.com/reinvent2017content-abd313/lab3/glue_9.PNG)

8. In this step we are going to add a new column indicating the type of Taxi through the Glue code so that we can combine Green and Yellow data. Because AWS Glue uses Apache Spark behind the scenes, you can easily switch from an AWS Glue DynamicFrame to a Spark DataFrame in the code and do advanced operations within Apache Spark.  Just as easily, you can switch back and continue to use the transforms and tables from the catalog. Make the following custom modifications in PySpark.

  i. Add the following header:

  ```
  from pyspark.sql.functions import lit
  from awsglue.dynamicframe import DynamicFrame
  ```

  ii. Find the last call before the the line that start with the datasink. This is the dynamic frame that is being used to write out the data. Let’s now convert that to a DataFrame.  Please replace the <DYNAMIC_FRAME_NAME> with the name generated in the script.


  ```
  ##----------------------------------
  #convert to a Spark DataFrame...
  yellowDF = <DYNAMIC_FRAME_NAME>.toDF()

  #add a new column for "type"
  yellowDF = yellowDF.withColumn("type", lit('yellow'))

  # Convert back to a DynamicFrame for further processing.
  yellowDynamicFrame = DynamicFrame.fromDF(yellowDF, glueContext, "yellowDF_df")
  ##----------------------------------
  ```
  iii. In the last datasink call, change the dynamic frame to point to the new custom dynamic frame created from the Spark DataFrame:

  ```
  datasink4 = glueContext.write_dynamic_frame.from_options(frame = yellowDynamicFrame, connection_type = "s3", connection_options = {"path": "s3://<YOURBUCKET/ AND PREFIX/>"}, format = "parquet", transformation_ctx = "datasink4")
  ```

9. Next step is to add code for the second data source: Green table. You can achieve this using a separate job but to save time we add additional code to the same job. Right before the 'job.commit()' add following code:

  ```
  ####### Second DataSource

  datasource22 = glueContext.create_dynamic_frame.from_catalog(database = "nycitytaxianalysis", table_name = "green", transformation_ctx = "datasource22")

  applymapping22 = ApplyMapping.apply(frame = datasource22, mappings = [("vendorid", "long", "vendorid", "long"), ("lpep_pickup_datetime", "string", "pickup_date", "timestamp"), ("lpep_dropoff_datetime", "string", "dropoff_date", "timestamp"), ("store_and_fwd_flag", "string", "store_and_fwd_flag", "string"), ("ratecodeid", "long", "ratecodeid", "long"), ("pulocationid", "long", "pulocationid", "long"), ("dolocationid", "long", "dolocationid", "long"), ("passenger_count", "long", "passenger_count", "long"), ("trip_distance", "double", "trip_distance", "double"), ("fare_amount", "double", "fare_amount", "double"), ("extra", "double", "extra", "double"), ("mta_tax", "double", "mta_tax", "double"), ("tip_amount", "double", "tip_amount", "double"), ("tolls_amount", "double", "tolls_amount", "double"), ("improvement_surcharge", "double", "improvement_surcharge", "double"), ("total_amount", "double", "total_amount", "double"), ("payment_type", "long", "payment_type", "long")], transformation_ctx = "applymapping2")

  resolvechoice22 = ResolveChoice.apply(frame = applymapping22, choice = "make_struct", transformation_ctx = "resolvechoice22")

  dropnullfields22 = DropNullFields.apply(frame = resolvechoice22, transformation_ctx = "dropnullfields22")

  ##----------------------------------
  #convert to a Spark DataFrame...
  greenDF = dropnullfields22.toDF()

  #add a new column for "type"
  greenDF = greenDF.withColumn("type", lit('green'))

  # Convert back to a DynamicFrame for further processing.
  greenDynamicFrame = DynamicFrame.fromDF(greenDF, glueContext, "greenDF_df")
  ##----------------------------------

  datasink2 = glueContext.write_dynamic_frame.from_options(frame = greenDynamicFrame, connection_type = "s3", connection_options = {"path": "s3://<YOURBUCKET/ AND PREFIX/>"}, format = "parquet", transformation_ctx = "datasink2")

  ####### End of Second DataSource
  ```

  > Ensure that you replace the bucket name and path with the S3 path that you want to store the Parquet data

  iv. Now click on **Save** and **Run Job**.

9. The code should look like [Glue Job Code](https://github.com/safipour/devday18/blob/master/glue-etl.py) with S3 bucket that you created earlier as the target.

![glue10](https://s3-us-west-2.amazonaws.com/reinvent2017content-abd313/lab3/glue_10.PNG)

10. In the parameters pop-up, for Job bookmark, ensure its **Enable** and click on **Run Job**.

11. This job will run for roughly around 2 minutes.

   ![glue11](https://s3-us-west-2.amazonaws.com/reinvent2017content-abd313/lab3/glue_11.PNG)

12. You can view logs on the bottom page of the same page.

  ![glue12](https://s3-us-west-2.amazonaws.com/reinvent2017content-abd313/lab3/glue_12.PNG)

13. The target folder (S3 Bucket) specified above (step 6 iii) will now have the converted parquet data.

## Query the Partitioned Data using Amazon Athena

In regions where AWS Glue is supported, Athena uses the AWS Glue Data Catalog as a central location to store and retrieve table metadata throughout an AWS account. The Athena execution engine requires table metadata that instructs it where to read data, how to read it, and other information necessary to process the data. The AWS Glue Data Catalog provides a unified metadata repository across a variety of data sources and data formats, integrating not only with Athena, but with Amazon S3, Amazon RDS, Amazon Redshift, Amazon Redshift Spectrum, Amazon EMR, and any application compatible with the Apache Hive metastore.

1. Open the [AWS Management console for Amazon Athena](https://us-west-2.console.aws.amazon.com/athena/home?force&region=us-west-2).

   > Ensure you are in the **Asia Pacific (Sydney)** region.

2. Under Database, you should see the database **nycitytaxi-devday18** which was created during the previous section.

3. Click on **Create Table** right below the drop-down for Database and click on **Automatically (AWS Glue Crawler)**.

4. You will now be re-directed to the AWS Glue console to set up a crawler. The crawler connects to your data store and automatically determines its structure to create the metadata for your table. Click on **Continue**.

5. Enter Crawler name as **nycitytaxi-crawlerparquet-devday18** and Click **Next**.

6. Select Data store as **S3**.

7. Choose Crawl data in **Specified path in my account**.

8. For Include path, click on the folder Icon and choose the **target** folder previously made which contains the parquet data and click on **Next**.

![glue18](https://s3-us-west-2.amazonaws.com/reinvent2017content-abd313/lab3/glue_18.PNG)

9. In Add another data store, choose **No** and click on **Next**.

10. For Choose an IAM role, select Choose an existing IAM role, and in the drop-down pick the role made in the previous section and click on **Next**.

11. In Create a schedule for this crawler, pick frequency as **Run on demand** and click on **Next**.

12. For Configure the crawler's output, Click **Add Database** and enter **nycitytaxi-devday18-parquet** as the database name and click **create**. For Prefix added to tables, you can enter a prefix **parq_** and click **Next**.

13. Review the Crawler Info and click **Finish**. Click on **Run it Now?**.

14. Click on **Tables** on the left, and for database nycitytaxi-devday18-parquet you should see the table parq_target. Click on the table name and you will see the MetaData for this converted table.

15. Open the [AWS Management console for Amazon Athena](https://us-west-2.console.aws.amazon.com/athena/home?force&region=us-west-2).

    > Ensure you are in the **US West (Oregon)** region.

16. Under Database, you should see the database **nycitytaxi-devday18-parquet** which was just created. Select this database and you should see under Tables **parq_target**.

17. In the query editor on the right, type

    ```
    select count(*) from parq_target;
    ```

    and take note the Run Time and Data scanned numbers here.

    ![glue19](https://s3-us-west-2.amazonaws.com/reinvent2017content-abd313/lab3/glue_comp_scanresult.PNG)

    What we see is the Run time and Data scanned numbers for Amazon Athena to **query and scan the parquet data**.

18. Under Database, you should see the earlier made database **nycitytaxi-devday18** which was created in a previous section. Select this database and you should see under Tables **devday18_yellow**.

19. In the query editor on the right, type

    ```
    select count(*) from devday18_yellow;
    ```

    and take note the Run Time and Data scanned numbers here.

    ![glue20](https://s3-us-west-2.amazonaws.com/reinvent2017content-abd313/lab3/glue_uncomp_scanresult.PNG)

20. What we see is the Run time and Data scanned numbers for Amazon Athena to query and scan the uncompressed data from the previous section.


> Note: Athena charges you by the amount of data scanned per query. You can save on costs and get better performance if you partition the data, compress data, or convert it to columnar formats such as Apache Parquet.

## Deleting the Glue database, crawlers and ETL Jobs created for this Lab

Now that you have successfully discovered and analyzed the dataset using Amazon Glue and Amazon Athena, you need to delete the resources created as part of this lab.

1. Open the [AWS Management console for Amazon Glue](https://us-west-2.console.aws.amazon.com/glue/home?region=us-west-2#). Ensure you are in the Oregon region (as part of this lab).
2. Click on **Databases** under Data Catalog column on the left.
3. Check the box for the Database that were created as part of this lab. Click on **Action** and select **Delete Database**. And click on **Delete**. This will also delete the tables under this database.
4. Click on **Crawlers** under Data Catalog column on the left.
5. Check the box for the crawler that were created as part of this lab. Click on **Action** and select **Delete Crawler**. And click on **Delete**.
6. Click on **Jobs** under ETL column on the left.
7. Check the box for the jobs that were created as part of this lab. Click on **Action** and select **Delete**. And click on **Delete**.
8. Open the [AWS Management console for Amazon S3](https://s3.console.aws.amazon.com/s3/home).
9. Click on the S3 bucket that was created as part of this lab. You need to click on its corresponding **Bucket icon** to select the bucket instead of opening the bucket. Click on **Delete bucket** button on the top, to delete the S3 bucket. In the pop-up window, Type the name of the bucket (that was created as part of this lab), and click **Confirm**.

## Summary

In the lab, you went from data discovery to analyzing a canonical dataset, without starting and setting up a single server. You started by crawling a dataset you didn’t know anything about and the crawler told you the structure, columns, and counts of records.

From there, you saw the datasets were in different formats, but represented the same thing: NY City Taxi rides. You then converted them into a canonical (or normalized) form that is easily queried through Athena and possible in QuickSight, in addition to a wide number of different tools not covered in this post.

---
## License

This library is licensed under the Apache 2.0 License.
