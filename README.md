#File descriptions
* dwh.cfg contains all configuration parameters and access keys
* create_tables.py contains functions to create all database tables
* sql_queries.py contains all queries to drop and create tables, insert and copy statements  
* etl.py is wrapper that runs all of the above 

#How to run the project?
Put in Redshift creditials including cluster address and run first create_tables.py and then etl.py. This makes postgres connection from Redshift cluster that loads data from song folder json files and user log json from s3 server. Staging tables called staging_events and staging_songs are created and data from the s3 bucket jsons is inserted. These are in turn used to populate tables songplays, songs, users, artists and time. Only sessions for which the user has listened at least one song are used.

#Star Schema description
The star schema represents 4 fact tables and one dimensional table. The fact tables are easily combined to the dimensional table on primary keys.
* users - fact table that connects to songplays by user_id
* songs- fact table that connects to songplays by song_id
* artists- fact table that connects to songplays by artist_id
* time- fact table that connects to songplays by start_time
* songplays - dimenstional table that contains all songplays by active and logged-in users


#Why should Sparkify use this ETL?
By using this star database schema, Sparkify can easily and intiutively analyze its users and their behavior. It can answer questions are which week days and day hours 
are the most active and which are the most popular songs and artists.