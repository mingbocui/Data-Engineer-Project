# US Tourism Behaviors Database

## Project Summary
This project focus on researching the tourism behaviors among the cities that non-immigrants visited and the cities demographics. We will process tha raw data and create relational database to store these data better.

## Datasets & Sources

- I94 Immigration Data: This data comes from the US National Tourism and Trade Office found [here](https://travel.trade.gov/research/reports/i94/historical/2016.html). Each report contains international visitor arrival statistics by world regions and select countries (including top 20), type of visa, mode of transportation, age groups, states visited (first intended address only), and the top ports of entry (for select countries).

- U.S. City Demographic Data: This dataset contains information about the demographics of all US cities and census-designated places with a population greater or equal to 65,000. Dataset comes from OpenSoft found [here](https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/).

- Airport Code Table: This is a simple table of airport codes and corresponding cities. The airport codes may refer to either IATA airport code, a three-letter code which is used in passenger reservation, ticketing and baggage-handling systems, or the ICAO airport code which is a four letter code used by ATC systems and for airports that do not have an IATA airport code (from wikipedia). It comes from [here](https://datahub.io/core/airport-codes#data).

## Motivations to build a Data Lake on S3:
- Easy access;
- Flexible design;
- Run peroidly with airflow, enable online analysis.

## Data Dictionary

### Data Model Justifications
To faciliate the access of the data, we decide to create a relational database, and normalize the tables to save sapce. Our model will be composed of one fact tables and several dimension tables.
More details will be illustracted in following data dictionaries.


### Fact Table 
#### immigrations

| Attribute      | Type    | Description     |
| ---------- | :-----------:  | :-----------: |
| cicid | FLOAT | CIC id | 
| i94yr | FLOAT | 4 digit year  |
| i94mon |FLOAT | Numeric month | 
| i94cit| FLOAT | City |
| i94res | FLOAT | Country code |
| i94port | VARCHAR | Airport code |
| arrdate  | FLOAT | Arrival Date in the USA |
| i94mode  | FLOAT | Mode to access |
| i94addr  |VARCHAR | State code |
|  depdate | FLOAT | Departure Date from the USA |
|  i94bir  | FLOAT | Age |
|  i94visa | FLOAT | Vias code |
|  count  | FLOAT | Used for summary statistics |
|  dtadfile | VARCHAR |  Character Date Field - Date added to I-94 Files | 
|  visapost | VARCHAR | Department of State where where Visa was issued |
|  occup | VARCHAR |  Occupation that will be performed in U.S. |
| entdepa | VARCHAR | Arrival Flag - admitted or paroled into the U.S.|
| entdepd | VARCHAR | Departure Flag - Departed, lost I-94 or is deceased 
| entdepu  | VARCHAR | Update Flag - Either apprehended, overstayed, adjusted to perm residence |
|  matflag  | VARCHAR | Match flag - Match of arrival and departure records |
|  biryear | FLOAT | 4 digit year of birth |
|  dtaddto | VARCHAR | Date to which admitted to U.S. |
| gender | VARCHAR | Non-immigrant sex |
| insnum | VARCHAR | INS number |
| airline | VARCHAR | Airline used to arrive in U.S. | 
| admnum | FLOAT | Admission Number |
| fltno | VARCHAR | Flight number of Airline used to arrive in U.S. |
| visatype | VARCHAR  | Class of admission legally admitting the non-immigrant to temporarily stay in U.S. |

### Dimension Tables
#### airport

| Attribute      | Type    | Description     |
| ---------- | :-----------:  | :-----------: |
| ident | VARCHAR | Airport id
| type | VARCHAR | Size of airport
| name | VARCHAR | name
| elevation_ft | float | Elevation in feet
| continent | VARCHAR  | Continet
| iso_country |  VARCHAR  | Country (ISO-2)
| iso_region | VARCHAR | region (ISO-2)
| municipality | VARCHAR | Municipality
| gps_code | VARCHAR | GPS code
| iata_code | VARCHAR | IATA code
| local_code | VARCHAR | Local code
| coordinates | VARCHAR  |  Coordinates
 
 #### i94cit

 | Attribute      | Type    | Description     |
 | ---------- | :-----------:  | :-----------: |
 | code | VARCHAR  | Country code
 | reason |  VARCHAR | Country
 
 
 #### i94port

 | Attribute      | Type    | Description     |
 | ---------- | :-----------:  | :-----------: |
 | code | VARCHAR | Entry airport code
 | port| VARCHAR | Airport city and state

 
 #### i94addr

 | Attribute      | Type    | Description     |
 | ---------- | :-----------:  | :-----------: |
 | code | VARCHAR  | State code
 | address |  VARCHAR | State
 
 #### i94visa

 | Attribute      | Type    | Description     |
 | ---------- | :-----------:  | :-----------: |
 | code | VARCHAR  | Visa code
 | visa |  VARCHAR | Visa description
 
 #### i94mode

| Attribute      | Type    | Description     |
| ---------- | :-----------:  | :-----------: |
| code | VARCHAR  | Transportation code
| model |  VARCHAR | Transportation description


#### us_cities_demographics
| Attribute      | Type    | Description     |
| ---------- | :-----------:  | :-----------: |
| city         |               VARCHAR | City
| state        |              VARCHAR | State
| median_age |                FLOAT | Median of age
| male_population |            FLOAT | Number of male population
| female_Population |          FLOAT | Number of female population
| total_Population     |      FLOAT | Number of total population
| number_veterans    |        FLOAT | Number of veterans
| foreign_born     |          FLOAT | Number of foreign born 
| average_household_size |    FLOAT | Average household size
| state_code         |        VARCHAR | State Code
| race | VARCHAR | Race
| count | INT | number


## ETL Pipeline DAG

This project aims to establish a whole pipeline to create tables from raw data. Some source files was exsisted in the format of `.sas` and can not be processed directly. The whole pipeline is listed as following steps:
- upload file to s3 bucket;
- extract sas data to csv and store data in s3 bucket;
- create tables in redshift;
- copy or insert data from csv to tables;
- data quality check.

The DAG created for the pipeline is:  
![](./images/dag.PNG)

## Add Airflow Connections
Here, we'll use Airflow's UI to configure your AWS credentials and connection to Redshift.

1. To go to the Airflow UI:  
- You can use the Project Workspace here and click on the blue Access Airflow button in the bottom right.
- If you'd prefer to run Airflow locally, open http://localhost:8080 in Google Chrome (other browsers occasionally have issues rendering the Airflow UI).

2. Click on the Admin tab and select Connections.
![](./images/admin-connections.png)  

3. Under Connections, select Create.
![](./images/create-connection.png)

4. On the create connection page, enter the following values:
- Conn Id: Enter `aws_credentials`.
- Conn Type: Enter `Amazon Web Services`.
- Login: Enter your Access key ID from the IAM User credentials you downloaded earlier.
- Password: Enter your Secret access key from the IAM User credentials you downloaded earlier.

Once you've entered these values, select Save and Add Another.
![](./images/connection-aws-credentials.png)
5. On the next create connection page, enter the following values:
- Conn Id: Enter `redshift`.
- Conn Type: Enter `Postgres`.
- Host: Enter the `endpoint of your Redshift cluster`, excluding the port at the end. You can find this by selecting your cluster in the Clusters page of the Amazon Redshift console. See where this is located in the screenshot below. IMPORTANT: Make sure to NOT include the port at the end of the Redshift endpoint string.
- Schema: Enter `dev`. This is the Redshift database you want to connect to.
- Login: Enter `awsuser`.
- Password: Enter the password you created when launching your Redshift cluster.
- Port: Enter `5439`.

Once you've entered these values, select Save.
![](./images/cluster-details.png)
![](./images/connection-redshift.png)

## Used Tools and Technologies
- upload file to S3 bucket;
- Process data directly on S3 and store the processed data in S3 bucket;
- create Redshift cluster and create tables;
- Load data from S3 to Redshift;
- Establish a data pipeline in Airflow to serialzie above operations by personalizing operators

## Other Scenarios
- The data was increased by 100x
If the data was enlarged by 100x, the bottleneck would be our redshift cluster size, we may need to create more nodes. We may also need to partition the dada by time or other key, which ensures the even distribution of data.

- The pipelines would be run on a daily basis by 7 am every day.
This problem could be solved by scheduling the Airflow to start regularly, it also can store some statistics of previous result.

- The database needed to be accessed by 100+ people.
The more people accessing the database the more cpu resources you need to get a fast experience. By using a distributed database we can improve our replications and partitioning to get faster query results for each user.


## Usage:
```console
# run to start airflow
airflow/start.sh
```
