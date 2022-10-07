class SqlQueries:
    songplay_table_insert = """
    INSERT INTO songplays VALUES (
        SELECT
                md5(events.sessionid || events.start_time) songplay_id,
                events.start_time,
                events.userid,
                events.level,
                songs.song_id,
                songs.artist_id,
                events.sessionid,
                events.location,
                events.useragent
                FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
            FROM staging_events
            WHERE page='NextSong') events
            LEFT JOIN staging_songs songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration
    )
    """

    user_table_insert = """
    INSERT INTO users VALUES (
        SELECT DISTINCT userid,
                        firstname,
                        lastname,
                        gender,
                        level
        FROM staging_events
        WHERE page='NextSong'
    )
    ON CONFLICT (userid) DO UPDATE SET level = EXCLUDED.level,
                                       first_name = EXCLUDED.first_name,
                                       gender = EXCLUDED.gender
    """

    song_table_insert = """
    INSERT INTO songs VALUES (
        SELECT DISTINCT song_id,
                        title,
                        artist_id,
                        year,
                        duration
        FROM staging_songs
    )
    """

    artist_table_insert = """
    INSERT INTO artists VALUES (
        SELECT DISTINCT artist_id,
                        artist_name,
                        artist_location,
                        artist_latitude,
                        artist_longitude
        FROM staging_songs
    )
    """

    time_table_insert = """
    INSERT INTO time VALUES (
        SELECT start_time,
               extract(hour from start_time),
               extract(day from start_time),
               extract(week from start_time),
               extract(month from start_time),
               extract(year from start_time),
               extract(dayofweek from start_time)
        FROM songplays
    )
    """

    quality_checks = [
        "SELECT COUNT(*) FROM songplays",
        "SELECT COUNT(*) FROM songs",
        "SELECT COUNT(*) FROM artists",
        "SELECT COUNT(*) FROM time",
        "SELECT COUNT(*) FROM users"
    ]
