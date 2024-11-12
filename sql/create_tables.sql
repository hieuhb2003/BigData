-- Create tables in PostgreSQL
CREATE TABLE IF NOT EXISTS daily_rankings (
    date DATE,
    rank INTEGER,
    rank_yesterday VARCHAR(10),
    release VARCHAR(255),
    daily_gross_clean NUMERIC,
    gross_change_day VARCHAR(50),
    percent_change_last_week VARCHAR(50),
    theaters INTEGER,
    average NUMERIC,
    gross_to_date_clean NUMERIC,
    days INTEGER,
    distributor VARCHAR(255),
    is_new BOOLEAN
);

CREATE TABLE IF NOT EXISTS movies (
    title VARCHAR(255) PRIMARY KEY,
    earliest_release_country VARCHAR(100),
    domestic_distributor VARCHAR(255),
    domestic_opening NUMERIC,
    earliest_release_date DATE,
    mpaa VARCHAR(10),
    running_time VARCHAR(50),
    genres TEXT,
    director VARCHAR(255),
    writer TEXT,
    producer TEXT,
    composer TEXT,
    cinematographer VARCHAR(255),
    editor VARCHAR(255),
    production_designer VARCHAR(255),
    actors TEXT
);
