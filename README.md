# Project Data Warehouse
## Project Overview
- A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. 
- Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app. 
- As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights into what songs their users are listening to.

![System Architecture](./images/System%20Architecture%20for%20AWS%20S3%20to%20Redshift%20ETL.png)

## Project Description
- In this project, you'll apply what you've learned on data warehouses and AWS to build an ETL pipeline for a database hosted on Redshift. 
- To complete the project
  - you will need to load data from S3 to staging tables on Redshift
  - execute SQL statements that create the analytics tables from these staging tables.

## Project Structure
Seminal Artefacts used on the project:
1. `scripts`
   2. `payload`
      3. `create_tables.py` re-create the tables
      4. `etl.py` main payload (stage data, transform data, load data)
      5. `sql_queries.py` contains all the SQL queries
   3. `setup`
      4. `cfn` - CloudFormation yaml files
      5. `iac` - build & teardown scripts + check for drift
   6. `troubleshoot` - any troubleshooting scripts


## Project Datasets
You'll be working with 3 datasets that reside in S3. Here are the S3 links for each:

- Song data: `s3://udacity-dend/song_data` 
- Log data: `s3://udacity-dend/log_data` 
- This third file `s3://udacity-dend/log_json_path.json` contains the meta information that is required by AWS to correctly load `s3://udacity-dend/log_data`

## Song Dataset
The first dataset is a subset of real data from the [Million Song Dataset](http://millionsongdataset.com/). Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID. For example, here are filepaths to two files in this dataset.

- `song_data/A/B/C/TRABCEI128F424C983.json`
- `song_data/A/A/B/TRAABJL12903CDCF1A.json`

And below is an example of what a single song file, `TRAABJL12903CDCF1A.json`, looks like.

```json
{
    "num_songs": 1, 
    "artist_id": "ARJIE2Y1187B994AB7", 
    "artist_latitude": null, 
    "artist_longitude": null, 
    "artist_location": "", 
    "artist_name": "Line Renaud", 
    "song_id": "SOUPIRU12A6D4FA1E1", 
    "title": "Der Kleine Dompfaff", 
    "duration": 152.92036, 
    "year": 0
}
```

## Log Dataset
The second dataset consists of log files in JSON format generated by this [event simulator](https://github.com/Interana/eventsim) based on the songs in the dataset above. These simulate app activity logs from a music streaming app based on specified configurations. The log files in the dataset you'll be working with are partitioned by year and month. For example, here are filepaths to two files in this dataset.

- `log_data/2018/11/2018-11-12-events.json`
- `log_data/2018/11/2018-11-13-events.json`

## Log Json Meta Information
`log_json_path.json` contains the meta information that is required by AWS to correctly load `log_data`. This file is used to specify the schema of the log data.

---
# Setup
- use [dwh.cfg](./dwh.cfg) to set up the AWS credentials and the S3 bucket name. In the project root folder.
- use [this shell script](./scripts/setup/iac/cfn_build_and_teardown.sh) to create the infrastructure on AWS using CloudFormation. In the project root folder. **THEN**
  - for now manually re-add the redshift role (NB. when adding role to the cluster or delete existing role but need to first `Cannot delete entity, must detach all policies first.`)
  - ensure you're user has `AmazonS3ReadOnlyAccess` & `AmazonRedshiftFullAccess` policies
  - make sure reachable from your IP
    - first check which AZ the cluster is available
    - then within the 'subnet group' note the subnet ID for that AZ
    - ensure there's an igw for that subnet
    - added inbound rule to clusters SG (**after off the VPN**) so accessible from my IP
- check you're able to access the dbase
- copy over s3 files from udacity
  - download s3 files `s3 cp s3://udacity-dend/log_data/2018/ --recursive .`
  - upload s3 files `s3 cp ./song_data s3://sjohal/udacity-dend/song_data --recursive`
---
# Payload
- create the tables `python3 create_tables.py`
- run the ETL `python3 etl.py`

--- 
# TODO
- [x] tidy up code
- [x] add comments
- [ ] check that dwh.cfg is not in git
- [x] check nothing sony related before zipping and sending to udacity
- [ ] commit and push to github
- [ ] create ERD diagram for final tables
- [ ] lessons learnt
  - sprint review or deep-dive (pre-recorded)
  - feet first
  - don't expect instructions to be perfect e.g. their Cloud Gateway
  - 
  