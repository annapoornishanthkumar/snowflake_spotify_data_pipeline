--creating a database
create or replace database Spotify;

--creating a schema
create or replace schema Spotify.Datapipeline;

--creating a table albums
create or replace table Spotify.Datapipeline.albums(
album_id string,
album_name string,
album_release_date date,
album_total_tracks int
);

--creating a table artists
create or replace table Spotify.Datapipeline.artists(
artist_id string,
artist_name string,
artist_url string
);

--creating a table songs
create or replace table Spotify.Datapipeline.songs(
song_id string,
song_name string,
duration int,
song_url string,
popularity int,
song_added_on datetime,
album_id string,
artist_id string
);

--Storage integration
create or replace storage integration spotify_connection
type = external_stage
storage_provider = s3
enabled = True
storage_aws_role_arn = 'arn:aws:iam::319501696719:role/spotify_snowflake_connection'
storage_allowed_locations = ('s3://spotify-etl-data-pipeline-snowflake/transformed_data/');

--getting the external_id and IAM_user_arn
desc integration spotify_connection;

--creating a file format for csv
create or replace file format Spotify.Datapipeline.spotify_csv_file_format
type = 'csv'
field_delimiter = ','
skip_header = 1
field_optionally_enclosed_by = '"';

--creating an external stage for albums
create or replace stage Spotify.Datapipeline.spotify_albums_stage
url = 's3://spotify-etl-data-pipeline-snowflake/transformed_data/album_data/'
storage_integration = spotify_connection
file_format = spotify_csv_file_format;

--creating an exterbal stage for artists
create or replace stage Spotify.Datapipeline.spotify_artists_stage
url = 's3://spotify-etl-data-pipeline-snowflake/transformed_data/artist_data/'
storage_integration = spotify_connection
file_format = spotify_csv_file_format;

--creating an external stage for songs
create or replace stage Spotify.Datapipeline.spotify_songs_stage
url = 's3://spotify-etl-data-pipeline-snowflake/transformed_data/songs_data/'
storage_integration = spotify_connection
file_format = spotify_csv_file_format;

--creating a snowpipe for albums
create or replace pipe spotify_album_snowpipe
auto_ingest = True
as
copy into Spotify.Datapipeline.albums
from @Spotify.Datapipeline.spotify_albums_stage;

--creating an event with s3 for albums
desc pipe spotify_album_snowpipe;

--creating a snowpipe for artists
create or replace pipe spotify_artist_snowpipe
auto_ingest = True
as
copy into Spotify.Datapipeline.artists
from @Spotify.Datapipeline.spotify_artists_stage;

--creating an event with s3 for artists
desc pipe spotify_artist_snowpipe;

--creating a snowpipe for songs
create or replace pipe spotify_song_snowpipe
auto_ingest = True
as
copy into Spotify.Datapipeline.songs
from @Spotify.Datapipeline.spotify_songs_stage;

--creating an event with s3 for artists
desc pipe spotify_song_snowpipe;

--Quering songs data
select * from songs;

--Quering albums data
select * from albums;

--Quering artists data
select * from artists;
