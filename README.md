# Data Modeling with Postgres

First data modeling project in the context of Udacity's Data Engineering
Nanodegree.

In this project, we build a Postgres DB for a mock startup called
***Sparkify*** for its music streaming app. The idea is to build a database to
help the company perform song play analysis.

We will design the database, implement it and build an ETL pipeline using
Python, loading some mock song and artist data together with some mock song
play logs.

## Database design

Given that the company's main focus is to do song play analysis, we decide to
focus our DB around the song plays.

As such, we decide to use a *star schema* with a main fact table **Songplays**,
that will hold each repdroduction of a song by a user. The dimension tables
which relate to this fact will be: **Songs**, to hold the infomration of each
song; **Artists**, to hold the information of each artist; **Users** to hold
the information of each user; and **Time** to hold the time of each song play.

Each of our fact and dimension tables will have its id column as primary key
(PK). Due to the nature of the given mock data, we use for the song and artist
id we use a PK of type varchar, for the user's id an int and for the each time
record we use the specific time as timestamp.

Ideally if the mock data were complete, that is all *log_data* reference songs
and artist which are in the *song_data*, we could reference each of the ids of
the dimension tables as foreign keys (FK) in the fact table and use their
combination as a PK. In our case, however, we decide to add a serial type
(unique not null auto-increasing int) as the Songplays fact table PK, ignoring
all FK relations.

The rest of names and data types come naturally from the mock data samples.

> NOTE: As a technical issue, the created database encoding (and an associated
> DB template) have been changed to UTF8, as the mock data provided contains
> characters that can not be encoded in ASCII.

## ETL design

For the ETL we have to different types of files:

- *song_data* JSONs which contain information about the songs and the artists.
    This data is processed to fill the *Songs* and *Artists* dimension tables.

- *log_data* JSONs which contain information about the song plays. This data is
    processed to fill the *Time* and *Users* dimension tables as well as the
    *Songplays* fact table.

Nevertheless, due to absence of song_id and artist_id in the log files, we need
to look them up in the dimensions tables. This proves to be a problem, as the
data in there are songs an artists in the log files which are not in the
*song_data* folder. To handle this issue, due to the nature of the mock data,
we decide to fill this songplays with **NULL** *song_id* and *artist_id*.
However, this would not happen in a real case with consistent data.


## Project structure

The project is divided in three parts:

- Python scripts *create_tables.py* and *etl.py* that respectively create and
    fill our **sparkifydb**, by using helper script *sql_queries.py*.

- Jupyter Notebooks *test.ipynb* and *etl.ipynb* used for testing and
    developing the code inside the Python scripts.

- Data folder which holds the song data, inside *song_data* folder; and song
    plays data, inside *log_data* folder. This data is used by the scripts to
    fill the DB.

## Project run instructions

To run the project, follow the steps:

1. Make sure the PostgreSQL deamon is running, there is a database called
    studentdb and a user with createdb permissions with username/pwd student
    to connect to.

2. Run the *create_tables.py* script from the root of the repository.

3. Run the *etl.py* script from the root of the repository.

## Song Play analysis query examples

Below are provided some example queries that can be executed over our DB to
perform song play analysis.

- Retrieve TOP 10 song plays of a given artist for a given year 2018:
```SQL
SELECT t.year, a.name, COUNT(*) as reproductions
FROM songplays s, time t, artists a
WHERE s.start_time = t.start_time
    AND s.artist_id = a.artist_id
    AND t.year = 2018
GROUP BY t.year, a.name
ORDER BY reproductions desc, a.name
LIMIT 10
```

- Retrieve the average length of the songs listened by each user for a given
    month Nov 2018:
```SQL
SELECT t.month, u.first_name, avg(s.length) as mean_song_length
FROM songplays s, time t, users u
WHERE s.start_time = t.start_time
    AND s.user_id = u.user_id
    AND t.year = 2018
    AND t.month = 11
GROUP BY t.month, u.first_name
ORDER BY mean_song_length desc, u.first_name
LIMIT 10
```