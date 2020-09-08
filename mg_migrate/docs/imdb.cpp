// Prvo skinuti name.basics, title.basics, title.principals, title.episode i title.ratings datoteke
// (moze ih se naci kad googlate IMDB dataset)
// Onda pokrenuti tako da postavite te datoteke
// Prije toga pripremite imdb bazu i ucitajte shemu imdb_schema.sql

#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "postgresql.hpp"

DEFINE_string(name_basics_tsv, "../data/name.basics.tsv", "Names");
DEFINE_string(title_basics_tsv, "../data/title.basics.tsv", "Titles");
DEFINE_string(title_principals_tsv, "../data/title.principals.tsv",
              "Principals");
DEFINE_string(title_episode_tsv, "../data/title.episode.tsv", "Episodes");
DEFINE_string(title_ratings_tsv, "../data/title.ratings.tsv", "Ratings");

std::vector<std::string> SplitBy(const std::string &str, char delim) {
  std::string token;
  std::vector<std::string> tokens;
  std::istringstream stream(str);
  while (std::getline(stream, token, delim)) {
    tokens.push_back(token);
  }
  return tokens;
}

void ReadTsv(
    const std::string &path,
    std::function<void(const std::vector<std::string> &row)> callback) {
  LOG(INFO) << "Reading TSV file: " << path;
  std::ifstream stream(path);
  CHECK(stream.is_open());
  std::string line;
  int count = 0;
  while (std::getline(stream, line)) {
    callback(SplitBy(line, '\t'));
    count++;
    if (count % 1000000 == 0) {
      LOG(INFO) << "Read " << count << " lines.";
    }
  }
  LOG(INFO) << "Completed " << count << " lines.";
  stream.close();
}

struct Actor {
  std::string actor_id;
  std::string name;
  std::string birth_year;
  std::string death_year;
};

struct Movie {
  std::string movie_id;
  std::string title;
  std::string year;
  std::string duration;
  std::vector<std::string> genres;
  std::string rating;
  // TODO: genres
};

struct Role {
  std::string actor_id;
  std::string title_id;
  std::vector<std::string> characters;
};

struct TVSeries {
  std::string series_id;
  std::string title;
  std::string start_year;
  std::string end_year;
  std::vector<std::string> genres;
  std::string rating;
};

struct TVEpisode {
  std::string series_id;
  std::string episode_id;
  std::string title;
  std::string duration;
  std::string season_number;
  std::string episode_number;
};

std::unordered_map<std::string, Actor> actors;
std::unordered_map<std::string, Movie> movies;
std::unordered_map<std::string, TVSeries> tvseries;
std::unordered_map<std::string, TVEpisode> tvepisodes;

std::unordered_map<std::string, std::vector<Role>> movie_to_actors;
std::unordered_map<std::string, std::vector<Role>> actor_to_movies;

std::unordered_map<std::string, std::vector<Role>> series_to_actors;
std::unordered_map<std::string, std::vector<Role>> actor_to_series;

Actor FindActorByName(const std::string &name) {
  for (const auto &[id, actor] : actors) {
    if (actor.name == name) return actor;
  }
  CHECK(false) << "Actor not found!";
  return Actor();
}

std::optional<Actor> FindActorById(const std::string &actor_id) {
  auto it = actors.find(actor_id);
  if (it == actors.end()) return std::nullopt;
  return it->second;
}

std::optional<Movie> FindMovieById(const std::string &movie_id) {
  auto it = movies.find(movie_id);
  if (it == movies.end()) return std::nullopt;
  return it->second;
}

std::optional<TVSeries> FindSeriesById(const std::string &series_id) {
  auto it = tvseries.find(series_id);
  if (it == tvseries.end()) return std::nullopt;
  return it->second;
}

std::optional<TVEpisode> FindEpisodeById(const std::string &episode_id) {
  auto it = tvepisodes.find(episode_id);
  if (it == tvepisodes.end()) return std::nullopt;
  return it->second;
}

std::string FixCharactersArray(const std::string &characters) {
  std::string ret = "";
  for (const auto &c : characters) {
    if (c == '[' || c == ']' || c == '\"') continue;
    ret.push_back(c);
  }
  return ret;
}

void ProcessActors() {
  ReadTsv(FLAGS_name_basics_tsv, [](const std::vector<std::string> &row) {
    const auto &id = row[0];
    const auto &name = row[1];
    const auto &birth_year = row[2];
    const auto &death_year = row[3];
    const auto professions = SplitBy(row[4], ',');
    // There is only one Kevin Bacon :)
    if (name == "Kevin Bacon" && id != "nm0000102") {
      return;
    }
    if (professions.empty()) return;
    if (professions[0] == "actor" || professions[0] == "actress") {
      actors[id] = {id, name, birth_year, Nullable(death_year)};
      return;
    }
    if (professions.size() >= 2 &&
        (professions[1] == "actor" || professions[1] == "actress")) {
      actors[id] = {id, name, birth_year, Nullable(death_year)};
    }
  });
  LOG(INFO) << "Found " << actors.size() << " actors.";
}

void ProcessTitles() {
  ReadTsv(FLAGS_title_basics_tsv, [](const std::vector<std::string> &row) {
    CHECK(row.size() == 9);
    const auto &id = row[0];
    const auto &type = row[1];
    const auto &title = row[2];
    const auto &start_year = row[5];
    const auto &end_year = row[6];
    const auto &duration = row[7];
    const auto genres = SplitBy(row[8], ',');
    if (type == "movie") {
      movies[id] = {id, title, start_year, Nullable(duration), genres, "NULL"};
    } else if (type == "tvSeries") {
      tvseries[id] = {id,     title, start_year, Nullable(end_year),
                      genres, "NULL"};
    } else if (type == "tvEpisode") {
      tvepisodes[id] = {"", id, title, Nullable(duration), "NULL", "NULL"};
    }
  });
  LOG(INFO) << "Found " << movies.size() << " movies.";
  LOG(INFO) << "Found " << tvseries.size() << " TV series.";
  LOG(INFO) << "Found " << tvepisodes.size() << " TV episodes.";
}

void ProcessEpisodes() {
  size_t count = 0;
  ReadTsv(FLAGS_title_episode_tsv,
          [&count](const std::vector<std::string> &row) {
            CHECK(row.size() == 4);
            const auto &episode_id = row[0];
            const auto &series_id = row[1];
            const auto &season_number = row[2];
            const auto &episode_number = row[3];
            if (FindSeriesById(series_id) == std::nullopt) return;
            auto episode_it = tvepisodes.find(episode_id);
            if (episode_it == tvepisodes.end()) return;
            episode_it->second.season_number = season_number;
            episode_it->second.episode_number = episode_number;
            episode_it->second.series_id = series_id;
            count++;
          });
  LOG(INFO) << "Found " << count << " episode-series relationships.";
}

void ProcessRatings() {
  size_t count = 0;
  ReadTsv(FLAGS_title_ratings_tsv,
          [&count](const std::vector<std::string> &row) {
            const auto &title_id = row[0];
            const auto &rating = row[1];
            auto movie_it = movies.find(title_id);
            if (movie_it != movies.end()) {
              movie_it->second.rating = rating;
              count++;
            }
            auto series_it = tvseries.find(title_id);
            if (series_it != tvseries.end()) {
              series_it->second.rating = rating;
              count++;
            }
          });
  LOG(INFO) << "Found " << count << " rated titles.";
}

void ProcessPrincipals() {
  size_t count_movies = 0;
  size_t count_series = 0;
  size_t actor_not_found = 0;
  ReadTsv(FLAGS_title_principals_tsv,
          [&count_movies, &count_series,
           &actor_not_found](const std::vector<std::string> &row) {
            CHECK(row.size() == 6);
            const auto &title_id = row[0];
            const auto &actor_id = row[2];
            const auto &category = row[3];
            const auto &characters = SplitBy(FixCharactersArray(row[5]), ',');
            if (characters == "\\N") return;
            auto actor = FindActorById(actor_id);
            if (actor == std::nullopt && category == "actor") actor_not_found++;
            if (actor == std::nullopt) return;
            auto movie = FindMovieById(title_id);
            if (movie != std::nullopt) {
              const Role role{actor_id, title_id, characters};
              movie_to_actors[title_id].push_back(role);
              actor_to_movies[actor_id].push_back(role);
              count_movies++;
            }
            auto series = FindSeriesById(title_id);
            if (series != std::nullopt) {
              const Role role{actor_id, title_id, characters};
              series_to_actors[title_id].push_back(role);
              actor_to_series[actor_id].push_back(role);
              count_series++;
            }
          });
  LOG(INFO) << "Found " << count_movies << " actor-movies relationships.";
  LOG(INFO) << "Found " << count_series << " actor-series relationships.";
  LOG(WARNING) << "Couldn't find " << actor_not_found << " actor roles.";
}

namespace traversal {

int cookie = 1;
std::unordered_map<std::string, int> visited_actors;
std::unordered_map<std::string, int> visited_movies;
std::unordered_map<std::string, int> visited_series;

// Skip movie and TV series titles with less than 4 actors.
const int kLowerBound = 4;

// forward declare:
void VisitActor(const std::string &actor_id, int depth);

void VisitMovie(const std::string &movie_id, int depth) {
  CHECK(FindMovieById(movie_id) != std::nullopt);
  if (movie_to_actors[movie_id].size() < kLowerBound) return;
  if (depth < 0) return;
  if (visited_movies[movie_id] == cookie) return;
  visited_movies[movie_id] = cookie;
  for (const auto &movie_role : movie_to_actors[movie_id]) {
    VisitActor(movie_role.actor_id, depth - 1);
  }
}

void VisitSeries(const std::string &series_id, int depth) {
  CHECK(FindSeriesById(series_id) != std::nullopt);
  if (series_to_actors[series_id].size() < kLowerBound) return;
  if (depth < 0) return;
  if (visited_series[series_id] == cookie) return;
  visited_series[series_id] = cookie;
  for (const auto &series_role : series_to_actors[series_id]) {
    VisitActor(series_role.actor_id, depth - 1);
  }
}

void VisitActor(const std::string &actor_id, int depth) {
  CHECK(FindActorById(actor_id) != std::nullopt);
  if (depth < 0) return;
  if (visited_actors[actor_id] == cookie) return;
  visited_actors[actor_id] = cookie;
  for (const auto &movie_role : actor_to_movies[actor_id]) {
    VisitMovie(movie_role.title_id, depth - 1);
  }
  for (const auto &series_role : actor_to_series[actor_id]) {
    VisitSeries(series_role.title_id, depth - 1);
  }
}

void TickCookie() { cookie++; }

}  // namespace traversal

namespace migrate {

const size_t kBatchSize = 100;

pqxx::connection conn{"postgresql://postgres:pass@localhost/imdb"};

std::string ToString(const std::string &str, char quote_symbol = '\'') {
  if (str == "NULL" || str == "\\N") return "NULL";
  return quote_symbol + conn.esc(str) + quote_symbol;
}

std::string ToNumber(const std::string &str) {
  if (str == "NULL" || str == "\\N") return "NULL";
  if (str == "") return "NULL";
  return str;
}

std::string ToArray(const std::vector<std::string> &vec) {
  std::ostringstream stream;
  stream << "'{";
  for (size_t i = 0; i < vec.size(); ++i) {
    if (i != 0) stream << ",";
    stream << ToString(vec[i], '\"');
  }
  stream << "}'";
  return stream.str();
}

void MigrateRows(const std::vector<std::vector<std::string>> &items,
                 const std::string &insert_stmt) {
  for (size_t i = 0; i < items.size(); i += kBatchSize) {
    pqxx::work tx{conn};
    std::ostringstream stream;
    stream << insert_stmt << " VALUES ";
    for (size_t j = i; j < std::min(items.size(), i + kBatchSize); ++j) {
      if (j != i) stream << ", ";
      stream << "(";
      const auto &vec = items[j];
      for (size_t k = 0; k < vec.size(); ++k) {
        if (k != 0) stream << ",";
        stream << vec[k];
      }
      stream << ")";
    }
    stream << ";";
    tx.exec0(stream.str());
    tx.commit();
  }
}

void MigrateActors(const std::unordered_map<std::string, int> &visited_actors) {
  std::vector<std::vector<std::string>> rows;
  for (const auto &[actor_id, _] : visited_actors) {
    auto actor = FindActorById(actor_id);
    CHECK(actor != std::nullopt);
    std::vector<std::string> values = {
        ToString(actor->actor_id), ToString(actor->name),
        ToNumber(actor->birth_year), ToNumber(actor->death_year)};

    rows.push_back(std::move(values));
  }
  LOG(INFO) << "Found " << rows.size() << " actors to migrate.";
  const std::string insert_stmt =
      "INSERT INTO actors (actor_id, name, birth_year, death_year)";
  MigrateRows(rows, insert_stmt);
}

void MigrateMovies(const std::unordered_map<std::string, int> &visited_movies) {
  std::vector<std::vector<std::string>> rows;
  for (const auto &[movie_id, _] : visited_movies) {
    auto movie = FindMovieById(movie_id);
    CHECK(movie != std::nullopt);
    std::vector<std::string> values = {
        ToString(movie->movie_id), ToString(movie->title),
        ToNumber(movie->year),     ToNumber(movie->duration),
        ToArray(movie->genres),    ToNumber(movie->rating)};
    rows.push_back(std::move(values));
  }
  LOG(INFO) << "Found " << rows.size() << " movies to migrate.";
  const std::string insert_stmt =
      "INSERT INTO movies (movie_id, title, year, duration, genres, rating)";
  MigrateRows(rows, insert_stmt);
}

void MigrateSeries(const std::unordered_map<std::string, int> &visited_series) {
  std::vector<std::vector<std::string>> rows;
  for (const auto &[series_id, _] : visited_series) {
    auto series = FindSeriesById(series_id);
    CHECK(series != std::nullopt);
    std::vector<std::string> values = {
        ToString(series->series_id),  ToString(series->title),
        ToNumber(series->start_year), ToNumber(series->end_year),
        ToArray(series->genres),      ToNumber(series->rating)};
    rows.push_back(std::move(values));
  }
  LOG(INFO) << "Found " << rows.size() << " TV series to migrate.";
  const std::string insert_stmt =
      "INSERT INTO tvseries (series_id, title, start_year, end_year, genres, "
      "rating)";
  MigrateRows(rows, insert_stmt);
}

void MigrateEpisodes(
    const std::unordered_map<std::string, int> &visited_series) {
  std::vector<std::vector<std::string>> rows;
  for (const auto &[_, episode] : tvepisodes) {
    if (visited_series.find(episode.series_id) == visited_series.end())
      continue;
    std::vector<std::string> values = {
        ToString(episode.series_id),     ToString(episode.episode_id),
        ToString(episode.title),         ToNumber(episode.duration),
        ToNumber(episode.season_number), ToNumber(episode.episode_number)};
    rows.push_back(std::move(values));
  }
  LOG(INFO) << "Found " << rows.size() << " TV episodes to migrate.";
  const std::string insert_stmt =
      "INSERT INTO tvepisodes (series_id, episode_id, title, duration, "
      "season_number, episode_number)";
  MigrateRows(rows, insert_stmt);
}

void MigrateMovieRoles(
    const std::unordered_map<std::string, int> &visited_actors,
    const std::unordered_map<std::string, int> &visited_movies) {
  std::vector<std::vector<std::string>> rows;
  for (const auto &[actor_id, _] : visited_actors) {
    for (const auto &role : actor_to_movies[actor_id]) {
      if (visited_movies.find(role.title_id) == visited_movies.end()) continue;
      std::vector<std::string> values = {ToString(actor_id),
                                         ToString(role.title_id),
                                         ToString(role.characters)};
      rows.push_back(std::move(values));
    }
  }
  LOG(INFO) << "Found " << rows.size() << " movie roles to migrate.";
  const std::string insert_stmt =
      "INSERT INTO movie_roles (actor_id, movie_id, characters)";
  MigrateRows(rows, insert_stmt);
}

void MigrateSeriesRoles(
    const std::unordered_map<std::string, int> &visited_actors,
    const std::unordered_map<std::string, int> &visited_series) {
  std::vector<std::vector<std::string>> rows;
  for (const auto &[actor_id, _] : visited_actors) {
    for (const auto &role : actor_to_series[actor_id]) {
      if (visited_series.find(role.title_id) == visited_series.end()) continue;
      std::vector<std::string> values = {ToString(actor_id),
                                         ToString(role.title_id),
                                         ToString(role.characters)};
      rows.push_back(std::move(values));
    }
  }
  LOG(INFO) << "Found " << rows.size() << " TV series roles to migrate.";
  const std::string insert_stmt =
      "INSERT INTO series_roles (actor_id, series_id, characters)";
  MigrateRows(rows, insert_stmt);
}

}  // namespace migrate

int main(int argc, char **argv) {
  gflags::SetUsageMessage("IMDb parser importer");
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  LOG(INFO) << "open: " << migrate::conn.is_open();

  ProcessActors();
  Actor kevin = FindActorByName("Kevin Bacon");
  LOG(INFO) << kevin.actor_id;
  LOG(INFO) << kevin.name;
  LOG(INFO) << kevin.birth_year;

  ProcessTitles();
  ProcessEpisodes();
  ProcessRatings();
  ProcessPrincipals();

  LOG(INFO) << "Kevin acted in " << actor_to_movies[kevin.actor_id].size()
            << " movies!";

  traversal::VisitActor("nm0000102" /* Kevin Bacon */, 4);
  traversal::TickCookie();
  traversal::VisitActor("nm0000288" /* Christian Bale */, 4);
  traversal::TickCookie();
  traversal::VisitSeries("tt0944947" /* Game of Thrones */, 3);

  for (const auto &[m_id, _] : traversal::visited_movies) {
    auto mov = FindMovieById(m_id);
    CHECK(mov != std::nullopt);
    LOG(INFO) << mov->title << " " << mov->rating;
  }

  LOG(INFO) << "Reduced to " << traversal::visited_actors.size() << " actors, "
            << traversal::visited_movies.size() << " movies and "
            << traversal::visited_series.size() << " TV series!";

  LOG(INFO) << "G Clooney: "
            << (traversal::visited_actors.find("nm0000123") !=
                traversal::visited_actors.end());
  LOG(INFO) << "J. Roberts: "
            << (traversal::visited_actors.find("nm0000210") !=
                traversal::visited_actors.end());

  migrate::MigrateActors(traversal::visited_actors);
  migrate::MigrateMovies(traversal::visited_movies);
  migrate::MigrateSeries(traversal::visited_series);
  migrate::MigrateEpisodes(traversal::visited_series);
  migrate::MigrateMovieRoles(traversal::visited_actors,
                             traversal::visited_movies);
  migrate::MigrateSeriesRoles(traversal::visited_actors,
                              traversal::visited_series);

  return 0;
}
