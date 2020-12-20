
1. Discuss the purpose of this database in context of the startup, Sparkify, and their analytical goals.
2. State and justify your database schema design and ETL pipeline.
[Optional] Provide example queries and results for song play ana



### To help the company to make who store data in S3, I will download song and log data from S3 and tranform the data into Star shema before loading them into redshift. By combining the datasets, data analysts can perform the queries to get insightful data in a more effectively. 

### First I create redshit clusters with local jupyter notebook with folloing steps

1. Create an iAM role with aws redshift database full and S3 bucket access
2. Create an Redshift with following python function 
        redshift.create_cluster(        
            #HW
            ClusterType=DWH_CLUSTER_TYPE,
            NodeType=DWH_NODE_TYPE,
            NumberOfNodes=int(DWH_NUM_NODES),

            #Identifiers & Credentials
            DBName=DWH_DB,
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
            MasterUsername=DWH_DB_USER,
            MasterUserPassword=DWH_DB_PASSWORD,
            
            #Roles (for s3 access)
            IamRoles=[roleArn]  
        )
   

***The command create the redshit cluster and produce a Cluster endpoint***
 
### Next, I connect the redshift clusters with its end points. After running create_table which will executes create_table_queries in sql_queries.py,  I have two staging tables, face and dimension tables in the redshift databases.

### In etl.py script, data will be downloaded from S3 buckets and load into the staging tables. All copied data will then tranform and load into the Star Schema tables created on previous step.  

### Problem solving

***During the process of running copy query, it is critical to have all the data in the correct order and format. One way to debug that is looking into the stl_load_errors table in the redshift pg_catalog schema. The results of the query will indicate the problem column as well as reason the query crashes.***

        select * from stl_load_errors

[COPY query document](https://docs.aws.amazon.com/redshift/latest/dg/r_COPY.html)


***Second problem I encountered : ts in log_data is in millisecond while start_time has to stored as timestamp in time table ***
***After I did some rearch, I figure out the solution to turn millisecon into timestamp (in the query operation) ***

        distinct ts,'1970-01-01'::date + ts/1000 * interval '1 second' as ts_datetime
        
***The time table requires hour, day, week, month, year, weekday for its columns, I found the EXTRACT(day, timestamp) method in postgre library which will extract the data out of the provided timestamp. ***

        ts_datetime, extract(HOUR FROM ts_datetime) as hour, extract(DAY FROM ts_datetime) as day, extract(week from ts_datetime) as week, extract(MONTH FROM ts_datetime) as month, extract(YEAR FROM ts_datetime) as year, extract(WEEKDAY FROM ts_datetime) as weekday 

##### Fact Table Reference

* songplays - records in event data associated with song plays i.e. records with page NextSong
    > songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

##### Dimension Tables Reference

* users - users in the app
    > user_id, first_name, last_name, gender, level
* songs - songs in music database
    > song_id, title, artist_id, year, duration
* artists - artists in music database
    > artist_id, name, location, lattitude, longitude
* time - timestamps of records in songplays broken down into specific units
    > start_time, hour, day, week, month, year, weekday

##### Other Info:


Million Song Dataset

The Million Songs Collection is a collection of 28 datasets containing audio features and metadata for a million contemporary popular music tracks.

US Snapshot ID (Linux/Unix): snap-5178cf30
Size: 500 GB
Source: http://labrosa.ee.columbia.edu/millionsong/
Created On: February 7, 2011
Last Updated: November 24, 2015


