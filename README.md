# Introduction

## Purpose of this project

Startup company, Sparkify, launched their new streaming app and collected song and user activities in JSON format, which resides in AWS S3. This project is to proivde a database and ETL pipeline that extracts the data from S3, processes them using Spark, and then loads the data back into S3 as a set of dimensional tables for analytics team to access and query thier data.

# Database schema design & ETL pipeline

## Database schema design

This project applies star schema consisting of following fact and dimension tables

### Fact table

1. songplays - holds log data associated with song plays, i.e. records with page `NextSong`
    - songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

### Dimension tables

1. users - holds app user data
    - user_id, first_name, last_name, gender, level
2. songs - holds song data
    - song_id, title, artist_id, year, duration
3. artists - holds artist data
    - artist_id, name, location, latitude, longitude
4. time - holds timestamp data in songplays table, the data broken down into specific units
    - start_time, hour, day, week, month, year, weekday

## ETL pipeline

Sparkify has two types of dataset (song and log dataset). This project creates two ETL pipiline for each dataset.

- ETL for Song dataset (`s3://udacity-dend/song_data/`)
    - Create dimention tables named **songs_table** and **artists_table** from song_data files in S3
- ETL for Log dataset (`s3://udacity-dend/log_data/`)
    - Create dimention tables named **time_table** and **users_table** as well as fact table, **songplays_table** from log_data files

# Files and instructions

## Files in this repository

- `etl.py` reads files from S3, processes the data with Spark, and then writes them back to S3
- `dl.cfg` contains the AWS credentials
- `command_EMR_cluster.md` includes terminal command to create EMR cluster and accees EMR master via SSH
- `README.md` discusses this project

## Run instructions

1. Launch EMR cluster in AWS
2. Run `submit-spark etl.py` to reads files from S3, processes the data with Spark, and writes partitioned parquet files in table directories onto S3