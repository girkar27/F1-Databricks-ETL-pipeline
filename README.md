# F1-Databricks-ETL-pipeline

Developed and designed a data pipeline from scratch on Azure Databricks. 
Data is fetched from external sources (APIs, CSV, Jsons) and ingested into an azure data lake. The ingested raw data is segregated, cleaned into tables an directories. 
The partioned data is then processed further joined and collected together from the processed storages and made ready for presenation. 
The presenation directory is maintained to join and collect the processed data together for analysts to join and present their dashboards. 

Pipeline: 

External sources ---> Ingestion ---> (RAW STORAGE) ---> processing ---> (Processed Storage) ----> Presenation actions --> (presenatation storage)  ---> Visualization


The required mounting of the the DL containers needed for to fetch information from the DL
Azure key vault was used to maintain user / admin access and additional data security.