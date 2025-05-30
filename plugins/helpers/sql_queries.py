class SqlQueries:
    songplay_table_insert = ("""
        INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
        SELECT DISTINCT
            TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second' AS start_time,
            se.userId AS user_id,
            se.level,
            ss.song_id,
            ss.artist_id,
            se.sessionId AS session_id,
            se.location,
            se.userAgent AS user_agent
        FROM staging_events se
        JOIN staging_songs ss
          ON se.song = ss.title
         AND se.artist = ss.artist_name
         AND se.length = ss.duration
        WHERE se.page = 'NextSong';
    """)

    user_table_insert = ("""
        INSERT INTO users (user_id, first_name, last_name, gender, level)
        SELECT DISTINCT userId, firstName, lastName, gender, level
        FROM staging_events
        WHERE userId IS NOT NULL;
    """)

    song_table_insert = ("""
        INSERT INTO songs (song_id, title, artist_id, year, duration)
        SELECT DISTINCT song_id, title, artist_id, year, duration
        FROM staging_songs;
    """)

    artist_table_insert = ("""
        INSERT INTO artists (artist_id, name, location, latitude, longitude)
        SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs;
    """)

    time_table_insert = ("""
        INSERT INTO time (start_time, hour, day, week, month, year, weekday)
        SELECT DISTINCT
            start_time,
            EXTRACT(hour FROM start_time),
            EXTRACT(day FROM start_time),
            EXTRACT(week FROM start_time),
            EXTRACT(month FROM start_time),
            EXTRACT(year FROM start_time),
            EXTRACT(weekday FROM start_time)
        FROM songplays;
    """)
