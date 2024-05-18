# Project Structure

- `README.md`: The markdown file contains a project overview and instructions.
- `ana_code`: Analysis code directory.
  - `Regression_Analysis.txt`: Regression analysis of the relationship between multiple factors: Poverty, Male Ratio, Asian Ratio, EnglishLearner Ratio, and Chronically Absence.

- `data_ingest`: Data ingestion instructions.
  - `Clean_Results`: Directory containing datasets after cleaning process.
  - `Joined_Results`: Directory containing the dataset after joining process.
  - `data_ingest.txt`: File include insturctions and code of data ingestion process.

- `etl_code`: Extract, Transform, Load code directory.
  - `Original_Data`: Directory containing original datasets.
  - `Clean`: Directory containing ETL code for `snapshots.csv` dataset by Yikai Ding.
    - `Clean.java` and `Clean.class`: Source code and class file for cleaning process.
    - `CleanMapper.java` and `CleanMapper.class`: Source code and class file for the Mapper.
    - `CleanReducer.java` and `CleanReducer.class`: Source code and class file for the Reducer.
    - `clean.jar`: Jar package.
    - `snapshot.csv`: The original dataset.
  - `Clean_Results`: Directory containing datasets after cleaning process.

- `profiling_code`: Profiling code directory.
  - `Profile`: Directory containing profiling code for `snapshots.csv` by Yikai Ding.
    - `UniqueRecs.java` and `UniqueRecs.class`: Source code and class file for generating unique records.
    - `UniqueRecsMapper.java` and `UniqueRecsMapper.class`: Source code and class file for the Mapper in genrating unique records process.
    - `UniqueRecsReducer.java` and `UniqueRecsReducer.class`: Source code and class file for the Reducer in genrating unique records process.
    - `uniqueRecs.jar`: Jar package.

- `screenshots`: Directory containing screenshots.
  - `Regression_Analysis_screenshots`: Screenshots about the regression analysis.
  - `Access_Permission_screenshots`: Screenshots about setting access permissions.

- `test_code`: Directory containing testing and unused code.
  - `FirstCode.scala`: Code containing pre-analysis process including basic stats and joining process.
  - `unused_code.txt`: Unused code.


# Building and running the Code

For data cleaning and profiling process, I implemented MapReduce tasks. You can find the source datasets under directories `/etl_code/Original_Data`. You can upload the code to NYU-dataproc. To complie and run the code, you can follow the instructions below. Please change the name of the code/file accordingly.

```bash
hdfs dfs -put NAME_OF_YOUR_FILE
javac -classpath `yarn classpath` -d . MAPPER_CLASS
javac -classpath `yarn classpath` -d . REDUCER_CLASS
javac -classpath `yarn classpath`:. -d . MAIN_CLASS
jar -cvf clean.jar *.class
hadoop jar clean.jar Clean NAME_OF_YOUR_FILE NAME_OF_YOUR_DIR
hdfs dfs -ls NAME_OF_YOUR_DIR
hdfs dfs -cat NAME_OF_YOUR_RESULT_DIR
```

For analysis part, I implemented MapReduce and Spark. You can follow the instructions in `/data_ingest/data_ingest.txt` to load the datasets. Then, you can run the regression analysis under `/ana_code/Regression_Analysis.txt`. The analysis code will generate some statistic data, such as R-suared, coefficient, Residuals...and the will be printed out directly.

# Where to find the data
- You can find the orginal data under `/etl_code/Original_Data`. Or you can browse the website `https://data.cityofnewyork.us/Education/2017-18-2021-22-Demographic-Snapshot/c7ru-d68s/about_data` and `https://data.cityofnewyork.us/Education/2016-17-2020-21-School-End-of-Year-Attendance-and-/gqq2-hgxd/about_data` to find the data.
- You can find the cleaned data under `/etl_code/Clean_Results`.
- You can find the joined dataset under project's root directory. Also you can go to HDFS to find it. `joined/part-00000-9c425ce7-e83a-4155-a86e-8ef8fd8daa59-c000.csv` and I have already set the access permission.