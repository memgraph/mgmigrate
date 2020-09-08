DROP TABLE IF EXISTS actors CASCADE;
CREATE TABLE actors (
  actor_id VARCHAR(20) PRIMARY KEY,
  name VARCHAR(50) NOT NULL,
  birth_year INT,
  death_year INT
);

DROP TABLE IF EXISTS movies CASCADE;
CREATE TABLE movies (
  movie_id VARCHAR(20) PRIMARY KEY,
  title VARCHAR(100) NOT NULL,
  year INT,
  duration INT,
  genres TEXT[],
  rating NUMERIC
);

DROP TABLE IF EXISTS tvseries CASCADE;
CREATE TABLE tvseries (
  series_id VARCHAR(20) PRIMARY KEY,
  title VARCHAR(100) NOT NULL,
  start_year INT,
  end_year INT,
  genres TEXT[],
  rating NUMERIC
);

DROP TABLE IF EXISTS tvepisodes CASCADE;
CREATE TABLE tvepisodes (
  series_id VARCHAR(20) NOT NULL,
  episode_id VARCHAR(20) PRIMARY KEY,
  title VARCHAR(300),
  duration INT,
  season_number INT,
  episode_number INT,
  FOREIGN KEY (series_id) REFERENCES tvseries(series_id)
);

DROP TABLE IF EXISTS movie_roles CASCADE;
CREATE TABLE movie_roles (
  actor_id VARCHAR(20) NOT NULL,
  movie_id VARCHAR(20) NOT NULL,
  characters TEXT,
  FOREIGN KEY (actor_id) REFERENCES actors(actor_id),
  FOREIGN KEY (movie_id) REFERENCES movies(movie_id)
);

DROP TABLE IF EXISTS series_roles CASCADE;
CREATE TABLE series_roles (
  actor_id VARCHAR(20) NOT NULL,
  series_id VARCHAR(20) NOT NULL,
  characters TEXT,
  FOREIGN KEY (actor_id) REFERENCES actors(actor_id),
  FOREIGN KEY (series_id) REFERENCES tvseries(series_id)
);



