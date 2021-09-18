# Project: Data Modeling with Postgres
 
## Task: create a database schema and ETL pipeline to analyse songs users are listening to

## PROJECT FILES
1. **test.ipynb**: displays the first few rows of each table to let you check your database.
2. **create_tables.py**: drops and creates your tables.
3. **etl.ipynb**: reads and processes a single file from song_data and log_data and loads the data into your tables. 
4. **etl.py**: reads and processes files from song_data and log_data and loads them into your tables. 
5. **sql_queries.py**: contains all sql queries, and is imported into the last three files above.
6. **README.md**: provides discussion on your project.

## DATA
### Song Dataset: 
a subset of real data from the Million Song Dataset in JSON format. 
### Filepaths: 
<img width="342" alt="Bildschirmfoto 2021-09-12 um 12 38 05" src="https://user-images.githubusercontent.com/63856362/133058291-2ada0fcc-bc65-46a1-a946-b055d7e4edbc.png">
 
### Example: 
{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}

### Log Dataset: 
log files in JSON format. 
### Filepaths: 
<img width="336" alt="Bildschirmfoto 2021-09-13 um 11 21 12" src="https://user-images.githubusercontent.com/63856362/133058754-da28f205-d381-42fa-a48d-c7291ca70dd2.png">

### Format: 
![LogData](https://video.udacity-data.com/topher/2019/February/5c6c15e9_log-data/log-data.png)
 
![Schema for Songplay Analysis](/Users/oxana_malyshkina/PycharmProjects/Data Engineering Udacity/sparkifydb_erd.png)

## RESULTS
In **test.ipynb** the following results can be seen:

<img width="440" alt="Bildschirmfoto 2021-09-13 um 11 23 11" src="https://user-images.githubusercontent.com/63856362/133059319-fe2dae17-6bde-4bbe-998c-b93b98d208e6.png">

<img width="489" alt="Bildschirmfoto 2021-09-13 um 11 24 00" src="https://user-images.githubusercontent.com/63856362/133059303-157514ba-da2e-4876-8e6f-2546fff2ccb6.png">