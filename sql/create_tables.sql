-- Staging tables
CREATE TABLE IF NOT EXISTS staging_events (
    artist          TEXT,
    auth            TEXT,
    firstName       TEXT,
    gender          TEXT,
    itemInSession   INTEGER,
    lastName        TEXT,
    length          FLOAT,
    level           TEXT,
    location        TEXT,
    method          TEXT,
    page            TEXT,
    registration    BIGINT,
    sessionId       INTEGER,
    song            TEXT,
    status          INTEGER,
    ts              BIGINT,
    userAgent       TEXT,
    userId          INTEGER
);

CREATE TABLE IF NOT EXISTS staging_songs (
    num_songs        INTEGER,
    artist_id        TEXT,
    artist_latitude  FLOAT,
    artist_longitude FLOAT,
    artist_location  TEXT,
    artist_name      TEXT,
    song_id          TEXT,
    title            TEXT,
    duration         FLOAT,
    year             INTEGER
);

-- Fact table
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id    VARCHAR PRIMARY KEY,
    start_time     TIMESTAMP NOT NULL,
    user_id        INTEGER NOT NULL,
    level          TEXT,
    song_id        TEXT,
    artist_id      TEXT,
    session_id     INTEGER,
    location       TEXT,
    user_agent     TEXT
);

-- Dimension tables
CREATE TABLE IF NOT EXISTS users (
    user_id    INTEGER PRIMARY KEY,
    first_name TEXT,
    last_name  TEXT,
    gender     TEXT,
    level      TEXT
);

CREATE TABLE IF NOT EXISTS songs (
    song_id   TEXT PRIMARY KEY,
    title     TEXT,
    artist_id TEXT,
    year      INTEGER,
    duration  FLOAT
);

CREATE TABLE IF NOT EXISTS artists (
    artist_id TEXT PRIMARY KEY,
    name      TEXT,
    location  TEXT,
    latitude  FLOAT,
    longitude FLOAT
);

CREATE TABLE IF NOT EXISTS time (
    start_time TIMESTAMP PRIMARY KEY,
    hour       INTEGER,
    day        INTEGER,
    week       INTEGER,
    month      INTEGER,
    year       INTEGER,
    weekday    INTEGER
);
