# Project
ETL pipeline for a data lake on local directory.

# Run
run_etl.ipynb

# Data
## Input
### Song profile data
`./data/song-data/*/*/*/*.json`
```
root
 |-- artist_id: string (nullable = true)
 |-- artist_latitude: double (nullable = true)
 |-- artist_location: string (nullable = true)
 |-- artist_longitude: double (nullable = true)
 |-- artist_name: string (nullable = true)
 |-- duration: double (nullable = true)
 |-- num_songs: long (nullable = true)
 |-- song_id: string (nullable = true)
 |-- title: string (nullable = true)
 |-- year: long (nullable = true)
```
### Song play log
`./data/log-data/*.json`
```
root
 |-- artist: string (nullable = true)
 |-- auth: string (nullable = true)
 |-- firstName: string (nullable = true)
 |-- gender: string (nullable = true)
 |-- itemInSession: long (nullable = true)
 |-- lastName: string (nullable = true)
 |-- length: double (nullable = true)
 |-- level: string (nullable = true)
 |-- location: string (nullable = true)
 |-- method: string (nullable = true)
 |-- page: string (nullable = true)
 |-- registration: double (nullable = true)
 |-- sessionId: long (nullable = true)
 |-- song: string (nullable = true)
 |-- status: long (nullable = true)
 |-- ts: long (nullable = true)
 |-- userAgent: string (nullable = true)
 |-- userId: string (nullable = true)
```
## Output
### Fact table
```
root
 |-- songplay_id: long (nullable = false)
 |-- ts: long (nullable = true)
 |-- user_id: string (nullable = true)
 |-- song_id: string (nullable = true)
 |-- artist_id: string (nullable = true)
 |-- location: string (nullable = true)
 |-- user_agent: string (nullable = true)
 |-- hour: integer (nullable = true)
 |-- day: integer (nullable = true)
 |-- week: integer (nullable = true)
 |-- month: integer (nullable = true)
 |-- year: integer (nullable = true)
 |-- weekday: integer (nullable = true)
```
### Dim table
```
# songs
root
 |-- song_id: string (nullable = true)
 |-- title: string (nullable = true)
 |-- artist_id: string (nullable = true)
 |-- duration: double (nullable = true)
# artists
root
 |-- artist_id: string (nullable = true)
 |-- artist_name: string (nullable = true)
 |-- artist_location: string (nullable = true)
 |-- artist_latitude: double (nullable = true)
 |-- artist_longitude: double (nullable = true)
# users
root
 |-- userId: string (nullable = true)
 |-- firstName: string (nullable = true)
 |-- lastName: string (nullable = true)
 |-- gender: string (nullable = true)
 |-- level: string (nullable = true)
# time table
root
 |-- ts: long (nullable = true)
 |-- hour: integer (nullable = true)
 |-- day: integer (nullable = true)
 |-- week: integer (nullable = true)
 |-- month: integer (nullable = true)
 |-- year: integer (nullable = true)
 |-- weekday: integer (nullable = true)
```

# search most popular artist for females
```
select a.artist_name, count(a.artist_name) from songs as s 
left join dim_user as u on s.user_id = u.userId 
left join dim_artist as a on s.artist_id = a.artist_id 
where u.gender = 'F'
GROUP BY a.artist_name
ORDER BY c desc
```

