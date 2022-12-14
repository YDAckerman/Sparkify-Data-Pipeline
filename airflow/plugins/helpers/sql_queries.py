# help from: https://docs.aws.amazon.com/redshift/latest/dg/merge-replacing-existing-rows.html

class SqlQueries:
    songplay_table_insert = """
    DELETE FROM songplays
    USING (SELECT DISTINCT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time
           FROM staging_events
           WHERE page='NextSong') AS tmp
    WHERE songplays.start_time = tmp.start_time;
    INSERT INTO songplays
    (start_time, userid, level, songid, artistid, sessionid,
     location, user_agent)
    SELECT
    events.start_time,
    events.userid,
    events.level,
    songs.song_id,
    songs.artist_id,
    events.sessionid,
    events.location,
    events.useragent
    FROM (
          SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
          FROM staging_events
          WHERE page='NextSong'
    ) events
    LEFT JOIN staging_songs songs
    ON events.song = songs.title
    AND events.artist = songs.artist_name
    AND events.length = songs.duration
    """

    user_table_insert = """
    DELETE FROM users
    USING (SELECT DISTINCT userid FROM staging_events WHERE page='NextSong') AS tmp
    WHERE users.userid = tmp.userid;
    INSERT INTO users (userid, first_name,
                       last_name, gender, level)
        SELECT
        userid,
        firstname,
        lastname,
        gender,
        level
        FROM (SELECT *,
              ROW_NUMBER() OVER (PARTITION by userid ORDER BY ts DESC) AS row_num
              FROM staging_events
              WHERE page='NextSong')
        WHERE row_num = 1;
    """

    song_table_insert = """
    INSERT INTO songs
    SELECT DISTINCT
    song_id,
    title,
    artist_id,
    year,
    duration
    FROM staging_songs
    """

    artist_table_insert = """
    INSERT INTO artists
    SELECT DISTINCT
    artist_id,
    artist_name,
    artist_location,
    artist_latitude,
    artist_longitude
    FROM staging_songs
    """

    time_table_insert = """
    INSERT INTO time
    SELECT DISTINCT
    start_time,
    extract(hour from start_time),
    extract(day from start_time),
    extract(week from start_time),
    extract(month from start_time),
    extract(year from start_time),
    extract(dayofweek from start_time)
    FROM songplays
    """

    quality_checks = [
        "SELECT COUNT(*) FROM songplays",
        "SELECT COUNT(*) FROM songs",
        "SELECT COUNT(*) FROM artists",
        "SELECT COUNT(*) FROM time",
        "SELECT COUNT(*) FROM users"
    ]
