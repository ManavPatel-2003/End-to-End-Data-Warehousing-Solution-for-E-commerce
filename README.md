# End-to-End-Data-Warehousing-Solution-for-E-commerce

![Alt text](https://github.com/ManavPatel-2003/End-to-End-Data-Warehousing-Solution-for-E-commerce/blob/main/images/ETL.png?raw=true)

This architecture consists of end-to-end ETL and analytics pipeline that includes: 
1.	**Data Sources**: The table shown below consists of different data entities along with the platform or format in which it is stored.

| Data | Source Platform/Format |
|:-----|:------:|
| customers    |   PostgreSQL    |
| inventory    |   PostgreSQL    |
| returns    |   PostgreSQL    |
| shipments    |   PostgreSQL    |
| orders    |   MySQL    |
| order_items    |   MySQL    |
| products    |   MySQL    |
| ad_clicks    |   MongoDB    |
| reviews    |   MongoDB    |
| sessions    |   MongoDB    |
| campaigns    |   CSV    |
| cart_activity    |   XLSX    |

3.	**Data Ingestion**: Python scripts were used to extract and format the data and then raw data was staged into Amazon S3 in Parquet format.<br><br>
4.	**Schema Detection and Querying**: AWS Glue Crawlers detects the schema so that raw data can be queried using Amazon Athena in case it is required.<br><br>
5.	**Transformation Layer**: Performs transformations according to requirements such as cleaning, joining, and deriving new columns and were performed using AWS Glue (PySpark scripts). Clean and transformed data was again stored in S3.<br><br>
6.	**Data Warehouse**: Clean and transformed data was loaded to Amazon Redshift Serverless from S3. Data warehouse is designed using a snowflake-like schema with fact and dimension tables.<br><br> 
7.	**Visualization & Reporting**: Power BI dashboards provided visuals for various business teams (sales, marketing, customer support).

<br><br><br><br>
## Sample Dashboards:<br><br>
1. Sales<br><br>
![Alt text](https://github.com/ManavPatel-2003/End-to-End-Data-Warehousing-Solution-for-E-commerce/blob/main/images/dashboards/sales2.png?raw=true)
  
3. Marketing<br><br>
![Alt text](https://github.com/ManavPatel-2003/End-to-End-Data-Warehousing-Solution-for-E-commerce/blob/main/images/dashboards/marketing2.png?raw=true)

5. Customers<br><br>
![Alt text](https://github.com/ManavPatel-2003/End-to-End-Data-Warehousing-Solution-for-E-commerce/blob/main/images/dashboards/customers2.png?raw=true)
