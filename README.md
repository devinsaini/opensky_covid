# opensky_covid
Udacity Data Engineer Nanodegree Capstone

In this project we design a data model and pipeline to analyze the impact of COVID-19 on flight traffic across the world.

### File description
config.cfg - declares path configurations for datasets

data_downloader.py - downloads the dataset files to local workspace

exploration.ipynb - Jupyter notebook to explore the datasets

pipeline.ipynb - data pipeline to convert the input datasets to parquet tables. It also demonstrates some basic data analysis using the final tables.

### Execution
- run data_downloader.py to download the datasets. Might need to update COVID-19 file name from AWS COVID-19 data lake /enigma-jhu-timeseries.

- start Jupyter server and run pipeline.ipynb


### Credits
Matthias Schäfer, Martin Strohmeier, Vincent Lenders, Ivan Martinovic and Matthias Wilhelm.
"Bringing Up OpenSky: A Large-scale ADS-B Sensor Network for Research".
In Proceedings of the 13th IEEE/ACM International Symposium on Information Processing in Sensor Networks (IPSN), pages 83-94, April 2014.