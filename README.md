## Engineering test exercise

Description, requirements and some sample files could be found in ___task___ folder.

### Assumptions made looking at data samples:

- In production environment I would expect big quantity of mid-size data files, because I see partitioned data
from the same source
- I would expect further structuring to subdirectories when quantity of files will increase
- I noted that entries in Combined.csv is sorted by IP. Not sure if that is really intended but I considered that as a requirement.
- Valid input file naming respects the pattern "AAA BBB.csv" or "AAA BBB part_id.csv". AAA and BBB stands for Region 
and Environment
- For solution name hereafter I will use Reducer, because by nature this exercise is typical data reduction task 

### Prerequisites
- Python 3.8+
- pandas installed
- Docker if you would like to use Reducer in dockerized form

### Configuration

Solution is fully configured through environment variables:
- REDUCER_INPUT_DIR - directory where inbound data files located
- REDUCER_OUTPUT_DIR - directory to where Combined.csv should be saved. Can be the same as 
input directory.
- LOG_LEVEL - one of "DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL". At DEBUG logging is really extensive.
- REDUCER_BATCH_SIZE - Reducer reads data from files for processing in batches. This is size of the batch in lines. 
This configuration element added to limit maximum memory usage.
- REDUCER_COMPACT_AT - Reducer compacts his result buffer when it reaches this amount of lines. This configuration
elements impacts tradeoff between "more memory" demand and "more CPU" need for often compacting. Having
statistics from the real data we could set this parameter reasonably.
- REDUCER_RUNNERS_QTY - Define how many parallel processes (runners) in total will be used to process input data files.
Positive integer >= 1.
- REDUCER_LOW_RUNNER and REDUCER_HIGH_RUNNER - defines range of the runners' number which will be started on this
specific instance of Reducer. This is to enable use of several instances in parallel e.g. orchestrated by Airflow on 
several compute nodes. For example on node A could be runners 1-4, on node B runners 5-8 and on high-CPU node C runners
9-24. So in total we will be executing a task on 24 CPUs to complete it faster.
- REDUCER_MODE - one of "reduce", "reduce_collect" or "collect". In "reduce" and "reduce_collect" modes Reducer spawns runners with IDs from 
REDUCER_LOW_RUNNER to REDUCER_HIGH_RUNNER. Each runner process peeks files for processing from the input directory and 
subdirectories by index sequence [ID, ID + 1 x REDUCER_RUNNERS_QTY, ID + 2 x REDUCER_RUNNERS_QTY, ...]. After processing
all his files runner saves partial result to output directory to file .Combined_ID.csv. Than in "reduce_collect" and
"collect" modes Reducer reads all partial results, aggregates them and save to Combined.csv in output directory. Please
look at ___airflow_dag_example.py___ for illustration how several Reducers in different modes could be combined togather 
to scale performance.

### Design reasoning

Python is a single-process language utilizing 1 CPU. Reducer being executed on 1 Ryzen zen2 CPU core @3.8 Hgz and with 
SMT=off able to process ~35Mb of data per second. Task looks definitely CPU intensive and storage bandwidth should not be 
limiting. E.g. quite typical for modern datacenters Melannox connectivity supports 40 - 100 GBit/s = 4 - 10GB / s = 144 - 285
Reducer processes. Thus for performance scaling Reducer should be able to run on many CPUs and looking at that 
"144 - 285 processes" CPUs could be on different hosts.

Taking into consideration scaling arguments described above it was decided to implement "Controller -> N x Runners" 
pattern with data partitioning by file and peeking by F(Runner_id). 

### Testing 

Reducer test suite contains 11 tests covering task requirements:
1. No valid data = no result
2. All files are valid, single dir
3. All files are valid, directory tree
4. Some files to be ignored
5. Broken csv file to be ignored
6. Csv file w/o required data column - to be ignored
7. No input directory
8. No output directory
9. Multiprocess config
10. New "aaa bbb.csv" file added and correctly processed
11. New "aaa bbb ccc.csv" file added and correctly processed

Please have a look at ___test_suite.py___ for more details. To support testing in automated CI / CD environment I
include ___Dockerfile.test___ to facilitate test step in build pipeline and example of use in ___misc/do_test.sh___
Depending on used IDE you could use test suit in it directly, e.g. in PyCharm I did so during development.

### Running in DEV

1. Install prerequisites
2. Set properly configuration environment variables. I used for that EnvFile plugin for PyCharm and dev.env 
configuration file.
3. Make sure datafiles are placed to input directory
4. python reducer.py

### Building and running in PROD

1. Please use ___Dockerfile.build___ for building production image in your CI / CD pipeline. I include an example of 
how to use it in ___misc/do_build.sh___
2. Tag image properly and push to your Docker registry
3. Please look at ___misc/reduce_it.sh___ example of how to execute Reducer from image on production host
4. Should you decide to scale Reducer to several hosts please look at ___misc/airflow_use_example.py___ of how to setup that
with Apache Airflow. Example assumes Airflow uses Celery executor and installed directly to hosts having docker. DAG 
can be easily modified for Airflow on Kubernets executor.

### Comments and alternative approach

For indeed massive integrations data exchange via host FS is for sure not the best approach. FS have limited amount of 
inodes which can be used for files and with the growth of the directory size FS performance usually degrades. I would 
recommend at that stage to consider switching to either Hadoop or some kind of blob storage for input capturing. For
further processing could be used Spark. I include working example of pyspark scrypt in ___misc/spark_example.py___ to 
illustrate how such processing could look like.
