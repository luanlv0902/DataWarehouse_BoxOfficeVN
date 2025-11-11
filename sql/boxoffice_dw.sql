
-- ==============================================
-- BOX OFFICE VIETNAM DATA WAREHOUSE SCHEMA
-- ==============================================

-- ==============================================
-- 1. DATABASE: db_control
-- ==============================================
CREATE DATABASE IF NOT EXISTS db_control;
USE db_control;

CREATE TABLE IF NOT EXISTS config (
    config_id INT AUTO_INCREMENT PRIMARY KEY,
    config_name VARCHAR(100),
    config_value VARCHAR(255),
    last_updated DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS etl_log (
    log_id INT AUTO_INCREMENT PRIMARY KEY,
    etl_step VARCHAR(100),
    start_time DATETIME,
    end_time DATETIME,
    status VARCHAR(50),
    message TEXT
);

-- ==============================================
-- 2. DATABASE: db_staging
-- ==============================================
CREATE DATABASE IF NOT EXISTS db_staging;
USE db_staging;

CREATE TABLE IF NOT EXISTS stg_boxoffice_raw (
    id INT AUTO_INCREMENT PRIMARY KEY,
    film_name VARCHAR(255),
    revenue_raw VARCHAR(50),
    tickets_raw VARCHAR(50),
    showtimes_raw VARCHAR(50),
    scraped_date DATE,
    source VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS stg_boxoffice_clean (
    film_name VARCHAR(255),
    revenue_vnd BIGINT,
    tickets_sold INT,
    showtimes INT,
    scraped_date DATE,
    etl_load_time DATETIME
);

-- ==============================================
-- 3. DATABASE: db_warehouse
-- ==============================================
CREATE DATABASE IF NOT EXISTS db_warehouse;
USE db_warehouse;

CREATE TABLE IF NOT EXISTS dim_movie (
    movie_key INT AUTO_INCREMENT PRIMARY KEY,
    movie_name VARCHAR(255),
    genre VARCHAR(100),
    release_date DATE,
    country VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS dim_date (
    date_key INT PRIMARY KEY,
    full_date DATE,
    year INT,
    month INT,
    day INT,
    quarter INT
);

CREATE TABLE IF NOT EXISTS fact_revenue (
    revenue_id INT AUTO_INCREMENT PRIMARY KEY,
    movie_key INT,
    date_key INT,
    revenue_vnd BIGINT,
    tickets_sold INT,
    showtimes INT,
    load_date DATETIME,
    FOREIGN KEY (movie_key) REFERENCES dim_movie(movie_key),
    FOREIGN KEY (date_key) REFERENCES dim_date(date_key)
);

-- ==============================================
-- 4. DATABASE: db_datamart
-- ==============================================
CREATE DATABASE IF NOT EXISTS db_datamart;
USE db_datamart;

CREATE TABLE IF NOT EXISTS dm_revenue_summary (
    date_key INT,
    total_revenue BIGINT,
    total_tickets INT,
    total_showtimes INT,
    movie_count INT,
    load_time DATETIME
);

CREATE TABLE IF NOT EXISTS dm_top_movies (
    rank INT,
    movie_name VARCHAR(255),
    revenue_vnd BIGINT,
    tickets_sold INT,
    showtimes INT,
    scraped_date DATE
);
