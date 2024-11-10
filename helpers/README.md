# Wikipedia Graph Database Deployment Tutorial

This deployment tutorial is for Linux users. If you want to know how to deploy the database on MacOS, please create an issue in the [GitHub repository](https://github.com/epfl-lts2/sparkwiki) of the project.

## 1. Set up the environment

1. Install JDK. **Open JDK 17** recommended.
2. Install **SBT** (Scala Build Tool)
    * [Linux installation details](https://www.scala-sbt.org/1.0/docs/Installing-sbt-on-Linux.html)
    * [MacOS installation details](https://www.scala-sbt.org/1.0/docs/Installing-sbt-on-Mac.html)
3. Install Apache **Spark**
    * We recommend using Apache Bigtop package manager to install Spark. Here is a [step-by-step tutorial](http://blog.miz.space/tutorial/2019/04/04/how-to-install-spark-using-apache-bigtop-ubuntu/) on how to do this. This option has been tested on Linux only.
    * Another way is to follow the steps described on the [Apache Spark website](https://spark.apache.org/downloads.html).
4. Install **Neo4J** (if you need to work with Wikipedia graph).
    * Follow the instructions on the [official website](https://debian.neo4j.org).
5. [**OPTIONAL**] Install **Apache Cassandra** (if you want to store a large number of pagecounts).
    * Follow the instructions on the [official website](http://cassandra.apache.org/download/)
6. Build the project.
    * Clone the repository from [GitHub](https://github.com/epfl-lts2/sparkwiki). The code prepares the data for the deployment.
    * Run `sbt package` to build the project and get the `.jar` file. We will need this file to run pre-processing jobs in Spark.

## 2. Download the data
For the sake of simplicity of this tutorial, let's say the local path to the data on your machine is the following: 

`/mnt/data/wikipedia/`

We are going to use it in all the deployment commands. Change the path in the commands accordingly when you run this tutorial on your machine.

### 2.1 SQL dumps
We will need the dumps to build Wikipedia graph.
1. Download SQL dumps [here](https://dumps.wikimedia.org/enwiki/). No need to download all the dumps though. Download only **page**, **redirect**, **category**, **categorylinks**, and **pagelinks** dumps:
    * enwiki-YYYYMMDD-**page**.sql.gz
    * enwiki-YYYYMMDD-**redirect**.sql.gz
    * enwiki-YYYYMMDD-**category**.sql.gz
    * enwiki-YYYYMMDD-**categorylinks**.sql.gz
    * enwiki-YYYYMMDD-**pagelinks**.sql.gz
2. Convert the `*.sql.gz` archives to `*.sql.bz` for performance reasons (s.t. Spark can process `.bz` archives in parallel). You can use the following comand to do this:
    `zcat dump.sql.gz | bzip2 > dump.sql.gz`

3. Put the files to `/mnt/data/wikipedia/dumps/`.

### 2.2 Pagecounts
1. Download pageviews archives [here](https://dumps.wikimedia.org/other/pageview_complete/). For example, if you need the data for 2019, choose the corresponding folder and download the data for the month(s) of your interest. 

    **Important note:** if you download archives of multiple months/years, put them in one folder (do not create separate folders for different months/years).

2. *If you have already downloaded the SQL dumps described in the previous section, skip this step.* 
    Download *enwiki-YYYYMMDD-**page**.sql.gz* SQL dump [here](https://dumps.wikimedia.org/enwiki/).

3. Put the files to `/mnt/data/wikipedia/pagecounts/`.

Now, everything is ready and you can pre-process the files. Note that you can use graph data and pagecounts separately. If you need only graph data, follow the instructions in Section 3. If you need only pagecounts data, follow the instructions in Section 4.

## 3. Deploy the graph database
### 3.1 Pre-process files

To pre-process the dumps we will need to run `spark-submit` command in the following format:

```
spark-submit \
--class ch.epfl.lts2.wikipedia.DumpProcessor \
--master 'local[*]' \
--executor-memory 20g \
--driver-memory 24g \
--packages org.rogach:scallop_2.12:4.1.0 \
/home/gyde/Documents/sparkwiki/target/scala-2.12/sparkwiki_2.12-1.0.0.jar \
--dumpPath /home/gyde/Documents/bzsets/20020620 \
--outputPath /home/gyde/Documents/sparkwiki/outputs \
--namePrefix enwiki-20240620
```


After running this command you will see Spark logs in the terminal. After some time (around 30 minutes), you will have pre-processed SQL dumps stored in `/mnt/data/wikipedia/dumps-pre-processed/`.

### 3.2 Create wikipedia.db database
The default database in Neo4J is `graph.db` which defaults to a database named 'neo4j'

### 3.3 Start/Stop Neo4J
Start/Stop Neo4J service to initialize the `neo4j.db`.

`sudo neo4j start`

`sudo neo4j stop`

### 3.4 Import the pre-processed files in Neo4J
**Note1:** Neo4J service should be down. Otherwise, the script will not work.

**Note2:** You can find header files in the `import` folder, which is located [here](https://github.com/epfl-lts2/sparkwiki/tree/master/helpers/import). **Before running this script, put it in the same folder with the `import` folder.**

* First we must get rid of nodes that have missing relationships. To do this open the filterbadnodes.py file and adjust the file locations within the file. It will take your processed dump files and filter them for bad relationships. At the end you should have 'filt' dumps alongside the other dumps. 

* Run the script below to import the pre-processed *and filtered* files into Neo4J. This step takes quite some time depending on your hardware (the amount of RAM and the type of the storage). For example, on a computer with 32 GB of RAM and an SSD (free space of around 10 GB required), it should take less than 30 minutes.

run the .sh file located in the helpers/import folder. The command looks like this and will need destinations updated:
```
#!/bin/sh
delim="\t"
data_dir="/home/gyde/Documents/sparkwiki/outputs"

# Collect the actual file paths for each type of data
normal_pages=$(ls $data_dir/page/normal_pages/part-*.csv.gz | tr '\n' ',')
category_pages=$(ls $data_dir/page/category_pages/part-*.csv.gz | tr '\n' ',')
pagelinks=$(ls $data_dir/pagelinks/part-*.csv.gz | tr '\n' ',')
categorylinks=$(ls $data_dir/categorylinks/part-*.csv.gz | tr '\n' ',')

# Remove trailing commas from the lists
normal_pages=${normal_pages%,}
category_pages=${category_pages%,}
pagelinks=${pagelinks%,}
categorylinks=${categorylinks%,}

# Check if any file list is empty and report missing files
if [ -z "$normal_pages" ]; then echo "Normal pages CSV files not found."; exit 1; fi
if [ -z "$category_pages" ]; then echo "Category pages CSV files not found."; exit 1; fi
if [ -z "$pagelinks" ]; then echo "PageLinks CSV files not found."; exit 1; fi
if [ -z "$categorylinks" ]; then echo "CategoryLinks CSV files not found."; exit 1; fi

sudo neo4j-admin database import full \
    --delimiter="\t" \
    --report-file=/tmp/import-wiki.log \
    --id-type=INTEGER \
    --nodes=Page=page_header.csv,"/home/gyde/Documents/sparkwiki/outputs/page/filt_normal_pages/*.csv.gz" \
    --nodes=Page:Category=page_header.csv,"/home/gyde/Documents/sparkwiki/outputs/page/filt_category_pages/*.csv.gz" \
    --relationships=LINKS_TO=pagelinks_header.csv,"/home/gyde/Documents/sparkwiki/outputs/filt_pagelinks/*.csv.gz" \
    --relationships=BELONGS_TO=categorylinks_header.csv,"/home/gyde/Documents/sparkwiki/outputs/filt_categorylinks/*.csv.gz" \
    --verbose \
    --overwrite-destination

```

* Start Neo4J database service.
`sudo neo4j start`

* Open Neo4J web interface.
`http://localhost:7474/browser/`

* Create indexes for PAGE_ID and PAGE_TITLE.
```
CREATE INDEX IF NOT EXISTS FOR (p:Page) ON (p.id);
CREATE INDEX IF NOT EXISTS FOR (p:Page) ON (p.title);
```
* Check the indexes are complete.
```
SHOW INDEXES;
```
* Test some queries.
Example: 
```
MATCH (p:Page)-[:BELONGS_TO*1..2]->(c:Category {title: 'Physics'}) 
WITH DISTINCT p AS p1 
RETURN p1.id, p1.title, labels(p1);
```

The graph is ready. If you don't need pagecounts, you are good to go. Otherwise, keep following the instructions and pre-process the pagecounts.

## 4. Process raw pagecounts
Use `ch.epfl.lts2.wikipedia.DumpParser` to get `.parquet` files for the `page.sql` dumps. To do this, run a command in the following format:

```
spark-submit 
--class ch.epfl.lts2.wikipedia.DumpParser 
--master 'local[*]' 
--executor-memory 10g 
--driver-memory 10g 
--packages    org.rogach:scallop_2.11:4.0.1
              <SPARKWIKI LOCATION>/sparkwiki/target/scala-2.11/sparkwiki_<VERSION>.jar 
--dumpFilePaths <PATH TO THE DUMPS, e.g. ~/data/dumps/enwiki-20190901-page.sql.bz2>
--dumpType page 
--outputPath /mnt/data/wikipedia/page.parquet 
--outputFormat parquet
```
### 4.1 Output as parquet files

Now, you can filter the pagecounts. You can choose the dates and languages of interest using `ch.epfl.lts2.wikipedia.PagecountProcessor`.

```
spark-submit 
--class ch.epfl.lts2.wikipedia.PagecountProcessor 
--master 'local[*]' 
--executor-memory 10g 
--driver-memory 10g 
--packages     org.rogach:scallop_2.11:4.0.1,
               com.typesafe:config:1.4.1
               <SPARKWIKI LOCATION>/sparkwiki/target/scala-2.11/sparkwiki_2.11-<VERSION>.jar
--config <SPARKWIKI LOCATION>/sparkwiki/config/pagecount.conf
--basePath <DATA PATH>/pagecounts/2018/2018-08
--startDate 2018-08-01
--endDate 2018-08-31
--pageDump <PATH TO PRE-PROCESSED FILES FROM ch.epfl.lts2.wikipedia.DumpParser>/page.parquet 
--languages <LIST OF LANGUAGE CODES>
--outputPath <OUTPUT DIRECTORY>/pagecount.parquet 
```

For more information on the parameters check out the [README](https://github.com/epfl-lts2/sparkwiki/blob/master/README.md#pagecount-processor).

Now, you can use `.parquet` files stored in `--outputPath`, if you want to use the data for further processing in Spark or any other framework that supports `.parquet` format.

### 4.2 Output as Cassandra database
#### 4.2.1 Deploy the pagecounts database
NB: These operations need to be one only **once** to set up the output database.
* Start Apache Cassandra
`sudo service cassandra start`

* Check IP of your Cassandra node
`nodetool status`

* Open Cassandra console
`cqlsh`

* Create keyspace
The query below will create a keyspace for a single-node environment.
```
CREATE KEYSPACE wikipedia
 WITH REPLICATION = {
  'class' : 'SimpleStrategy',
  'replication_factor' : 1
 };
```
If you want to configure a multi-node environment, read more about replication strategies [here](https://docs.datastax.com/en/cql/3.3/cql/cql_reference/cqlCreateKeyspace.html).

* Create tables to import pagecounts
```
CREATE TABLE wikipedia.page_visits (
    page_id bigint,
    visit_time timestamp,
    count int,
    PRIMARY KEY (page_id, visit_time));
```

```
CREATE TABLE wikipedia.pagecount_metadata (
    start_time timestamp,
    end_time timestamp,
    PRIMARY KEY (start_time, end_time));
```

* Exit `cqlsh`

#### 4.2.2 Process pagecounts
Now, you can filter the pagecounts. You can choose the dates and languages of interest using `ch.epfl.lts2.wikipedia.PagecountProcessor`.

```
spark-submit 
--class ch.epfl.lts2.wikipedia.PagecountProcessor 
--master 'local[*]' 
--executor-memory 10g 
--driver-memory 10g 
--packages     org.rogach:scallop_2.11:4.0.1,
               com.datastax.spark:spark-cassandra-connector_2.11:2.4.2,
               com.typesafe:config:1.4.1
               <SPARKWIKI LOCATION>/sparkwiki/target/scala-2.11/sparkwiki_2.11-<VERSION>.jar
--config <SPARKWIKI LOCATION>/sparkwiki/config/pagecount.conf
--basePath <DATA PATH>/pagecounts/2018/2018-08
--startDate 2018-08-01
--endDate 2018-08-31
--pageDump <PATH TO PRE-PROCESSED FILES FROM ch.epfl.lts2.wikipedia.DumpParser>/page.parquet 
--languages <LIST OF LANGUAGE CODES>
```

The database connection information must be set in the `pagecount.conf` configuration file.
For more information on the parameters check out the [README](https://github.com/epfl-lts2/sparkwiki/blob/master/README.md#pagecount-processor).
The `--outputPath` option must not be set, otherwise `sparkwiki` will revert to parquet output.

#### 4.2.3 Verify the import. Show the table with pagecounts
NB: Check that your Cassandra server is running prior running the commands below

`cqlsh`

`select * from wikipedia.page_visits limit 10;`

`exit`
