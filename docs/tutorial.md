# Migrating IMDb from PostgreSQL to Memgraph

In this tutorial, we are going to migrate part of the IMDb database from PostgreSQL to Memgraph. To do this, we will use the `mg_migrate` tool, which goal is to import data to Memgraph from various types of source databases. PostgreSQL is a relational database as opposed to Memgraph which is a graph database, so the two databases differ in structure and query languages. Throughout the tutorial we are going to show how the relational structure can be converted to graph structure and point out some advantages of using a graph model by demonstrating queries that are easier to write and execute in Memgraph than in PostgreSQL. We also emphasize that purpose of the `mg_migrate` tool is prototyping with small datasets instead of using it for large scale database in production.

## Dataset

To create a source PostgreSQL database, we are going to need `imdb.sql`. The dataset is based on the official [IMDb dataset](https://www.imdb.com/interfaces/). However, since the original dataset is quite large, it is shrinked for tutorial purposes and may not be very accurate.

Assuming we created an empty PostgreSQL database `imdb`, we can run the following command to fill the database with data:

```
psql -U postgres -h localhost -d imdb < imdb.sql
```

The database consists of 6 tables with the following schema:
TODO: Replace SQL schema with nice illustration.

```sql
CREATE TABLE actors (
  actor_id VARCHAR(20) PRIMARY KEY,
  name VARCHAR(50) NOT NULL,
  birth_year INT,
  death_year INT
);

CREATE TABLE movies (
  movie_id VARCHAR(20) PRIMARY KEY,
  title VARCHAR(100) NOT NULL,
  year INT,
  duration INT,
  genres TEXT[],
  rating NUMERIC
);

CREATE TABLE tvseries (
  series_id VARCHAR(20) PRIMARY KEY,
  title VARCHAR(100) NOT NULL,
  start_year INT,
  end_year INT,
  genres TEXT[],
  rating NUMERIC
);

CREATE TABLE tvepisodes (
  series_id VARCHAR(20) NOT NULL,
  episode_id VARCHAR(20) PRIMARY KEY,
  title VARCHAR(300),
  duration INT,
  season_number INT,
  episode_number INT,
  FOREIGN KEY (series_id) REFERENCES tvseries(series_id)
);

CREATE TABLE movie_roles (
  actor_id VARCHAR(20) NOT NULL,
  movie_id VARCHAR(20) NOT NULL,
  characters TEXT[],
  FOREIGN KEY (actor_id) REFERENCES actors(actor_id),
  FOREIGN KEY (movie_id) REFERENCES movies(movie_id)
);

CREATE TABLE series_roles (
  actor_id VARCHAR(20) NOT NULL,
  series_id VARCHAR(20) NOT NULL,
  characters TEXT[],
  FOREIGN KEY (actor_id) REFERENCES actors(actor_id),
  FOREIGN KEY (series_id) REFERENCES tvseries(series_id)
);
```

## Migrating to Memgraph

The time has come to switch to Memgraph! First, we are going to run a Memgraph instance. In many cases there will be edges with properties, so make sure to enable that option:

```
memgraph --storage-properties-on-edges
```

We can now migrate data by running the following command:

```
mg_migrate --source-kind=postgresql \
           --source-host=localhost \
           --source-port=5432 \
           --source-username=postgresql \
           --source-password=pass \
           --source-database=imdb \
           --destination-host=localhost \
           --destination-port=7687 \
           --destination-use-ssl=false
```

*Almost all* tables from the relational database represent different *node types* with nodes corresponding to rows of the table. Migrating the relational IMDb scheme to Memgraph will result in four node types: `actors`, `movies`, `tvseries` and `tvepisodes`. These *node types* correspond to label names for nodes. Example nodes are: `(:actors {actor_id: ‘nm0000093’, name: ‘Brad Pitt’, birth_year: 1963})`, `(:movies {movie_id: ‘tt0110413’, title: ‘Léon: The Professional’, year: 1994, rating: 8.5, duration: 110, genres: [‘Action’, ‘Crime’, ‘Drama’]})` and `(:tvepisodes {series_id: ‘tt0944947’, episode_id: ‘tt1480055’, title: ‘Winter Is Coming’, duration: 62, season_number: 1 , episode_number: 1})`.

As we can see, the `series_id` field of `tvepisodes` table has a foreign key that references the `tvseries` table. That means that there is a connection between `tvepisodes` and `tvseries` and naturally we expect to have an edge between the two node types in the graph model. `mg_migrate` will create an edge without properties for *almost all* foreign key constraints pointing from a child table to a parent table. In our case, there will be an edge pointing from `tvepisodes` node to corresponding `tvseries` node (the edge type will be labeled simply as `tvepisodes_to_tvseries`).

There is one exception for tables and its foreign keys where the above rules don’t apply: **join tables**. Join tables in a relational database connect multiple tables (or form a recursive relationships). Instead of treating join tables that contain **exactly two** foreign keys as nodes, `mg_migrate` will transform rows of such tables as edges connecting nodes of corresponding foreign keys. Columns of join tables that are not part of any foreign key will be used as properties on edges. In our example, `movie_roles` and `series_roles` are join tables that consist of two foreign keys. For each row in `movie_roles` there will be created an edge pointing from `actors` node to `movies` node having `characters` property, e.g. `(:actors {name: ‘Christian Bale’})-[:movie_roles {characters: [‘Bruce Wayne’, ‘Batman’]->(:movies {title: ‘Batman Begins’})}]`.

## Sample queries

1. Let’s find all movies with an IMDb score of at least 8 casting Christian Bale. First, recall how this could be done in PostgreSQL:

```
SELECT title, rating
FROM movies
  JOIN movie_roles USING (movie_id)
  JOIN actors USING (actor_id)
WHERE actors.name = 'Christian Bale'
  AND rating >= 8
ORDER BY rating DESC;
```

Transforming this to an OpenCypher query:
```
MATCH (a:`actors` {name: ‘Christian Bale’})-[:movie_roles]->(m:`movies`)
WHERE m.rating >= 8
RETURN m.title, m.rating
ORDER BY m.rating DESC;
```

You can see for yourself that both queries return the same result:
```
         title         | rating
-----------------------+--------
 The Dark Knight       |    9.0
 The Prestige          |    8.5
 The Dark Knight Rises |    8.4
 Batman Begins         |    8.2
 Ford v Ferrari        |    8.1
```

2. Listing all movies where Brad Pitt and George Clooney casted together:

```
MATCH (:actors {name: ‘Brad Pitt’})-[e1]->(m:movies)<-[e2]-(:actors {name: ‘George Clooney’})
RETURN m.title;
```

3. Listing number of episodes for each Game of Thrones season:

```
MATCH (:tvseries {title: ‘Game of Thrones’})<-[:tvepisodes_to_tvseries]-(e:tvepisodes)
RETURN e.season_number, COUNT(e.episode_number);
```

4. Listing all constraints: `SHOW CONSTAINT INFO`.

In addition to migrate nodes and edges, `mg_migrate` will also migrate constraints. `unique` constraints can be recognized by `UNIQUE` keyword, while `existence` constraints are recognized by `NOT NULL` keyword in SQL. For primary keys both `unique` and `existence` constraints will be migrated.


### Kevin Bacon boosts Memgraph!

We are now going to demonstrate a query which is a lot easier to construct in OpenCypher than in SQL by introducing the Kevin Bacon number. Kevin Bacon is an actor whose number is popular in culture and represents a value recursively assigned to each actor the following way:
1. Kevin Bacon’s Kevin Bacon number is 0.
2. Kevin Bacon number of all actors that were directly involved in a movie or TV series with Kevin Bacon is 1.
3. Other actors that were casting together with them have Kevin Bacon number 2.
4. And so on...

We are now going to find a Kevin Bacon number of George Clooney. First, note that path of nodes from ‘George Clooney’ to ‘Kevin Bacon’ alternates between actors and movies/series:

```
(‘Goerge Clooney’) --[role]-->(movie/series)<--[role]--(actor1)--...--(actor2)--[role]-->(movie/series)<--[role]--(‘Kevin Bacon’)
```

We can use the BFS algorithm on graphs to find the shortest path from two nodes and use the following query to get the Kevin Bacon number of George Clooney:

```
MATCH path = (:actors {name: ‘George Clooney’})-[* bfs]-(:actors {name: ‘Kevin Bacon’})
RETURN size(path) / 2;
```

Instead of returning the number (`size(path) / 2`), we can directly see how the two actors are related by returning `path`. There we can see that Kevin Bacon casted together with Julia Roberts in *Flatliners*, who casted with George Clooney in *Money Monster* (among other movies).

