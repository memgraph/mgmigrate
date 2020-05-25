#include <gtest/gtest.h>

#include "memgraph_destination.hpp"
#include "memgraph_client.hpp"

using Query = std::pair<std::string, std::map<std::string, mg::Value>>;

const Query kCreateInternalIndex = {
    "CREATE INDEX ON :`__mg_vertex__`(`__mg_id__`);", {}};
const Query kDropInternalIndex = {"DROP INDEX ON :__mg_vertex__(__mg_id__);",
                                  {}};
const Query kRemoveInternalLabelProperty = {
    "MATCH (u) REMOVE u:__mg_vertex__, u.__mg_id__;", {}};

const std::string kCreateEdgeMatch =
    "MATCH (u:__mg_vertex__), (v:__mg_vertex__)";

/// Fake Memgraph client that saves all commands that were supposed to be
/// executed by the MemgraphDestination.
class FakeClient : public MemgraphClient {
 public:
  explicit FakeClient(std::vector<Query> *queries) : queries_(queries) {}

  bool Execute(const std::string &statement,
               const std::map<std::string, mg::Value> &params = {}) override {
    // In most cases, none results are expected by the execution. However, in
    // the case of edge creation, the client expects one row to be returned.
    if (statement.size() > kCreateEdgeMatch.size() &&
        statement.substr(0, kCreateEdgeMatch.size()) == kCreateEdgeMatch) {
      fake_fetch_result_ = true;
    }
    queries_->emplace_back(statement, params);
    return true;
  }

  std::optional<std::vector<mg::Value>> FetchOne() override {
    if (fake_fetch_result_) {
      fake_fetch_result_ = false;
      return std::vector<mg::Value>{};
    }
    return std::nullopt;
  }

 private:
  std::vector<Query> *queries_;

  bool fake_fetch_result_{false};
};

/// Helper function which returns the queries executed by actions given in the
/// `callback`. It creates a new `MemgraphDestination` object using the fake
/// client.
std::vector<Query> Execute(
    std::function<void(MemgraphDestination *)> callback) {
  std::vector<Query> queries;
  {
    MemgraphDestination dest(
        std::unique_ptr<MemgraphClient>(new FakeClient(&queries)));
    callback(&dest);
  }
  return queries;
}

TEST(MemgraphDestinationTest, CreateVertex) {
  auto queries = Execute([](MemgraphDestination *dest) {
    dest->CreateVertex({mg::Id::FromInt(1), {}, {}});
    dest->CreateVertex({mg::Id::FromInt(2), {"label1"}, {}});
    dest->CreateVertex({mg::Id::FromInt(3), {}, {{"prop1", "value1"}}});
    dest->CreateVertex({mg::Id::FromInt(4), {"label1", "label2"}, {}});
    dest->CreateVertex(
        {mg::Id::FromInt(5), {"label1"}, {{"prop1", 3.14}, {"prop2", false}}});
  });

  const std::vector<Query> expected = {
      kCreateInternalIndex,
      {"CREATE (u:__mg_vertex__ {__mg_id__: 1});", {}},
      {"CREATE (u:__mg_vertex__:`label1` {__mg_id__: 2});", {}},
      {"CREATE (u:__mg_vertex__ {__mg_id__: 3, `prop1`: $param0});",
       {{"param0", "value1"}}},
      {"CREATE (u:__mg_vertex__:`label1`:`label2` {__mg_id__: 4});", {}},
      {"CREATE (u:__mg_vertex__:`label1` {__mg_id__: 5, `prop1`: "
       "$param0, `prop2`: $param1});",
       {{"param0", 3.14}, {"param1", false}}},
      kDropInternalIndex,
      kRemoveInternalLabelProperty};

  ASSERT_EQ(queries, expected);
}

TEST(MemgraphDestinationTest, CreateEdge) {
  auto queries = Execute([](MemgraphDestination *dest) {
    dest->CreateVertex({mg::Id::FromInt(1), {}, {}});
    dest->CreateVertex({mg::Id::FromInt(2), {}, {}});
    dest->CreateEdge({mg::Id::FromInt(1),
                      mg::Id::FromInt(1),
                      mg::Id::FromInt(2),
                      "link",
                      {}});
    dest->CreateEdge(
        {mg::Id::FromInt(2),
         mg::Id::FromInt(2),
         mg::Id::FromInt(1),
         "edge",
         {{"prop1", mg::Value()}, {"prop2", std::vector<mg::Value>{1, 2}}}});
    dest->CreateEdge({mg::Id::FromInt(3), mg::Id::FromInt(1),
                      mg::Id::FromInt(1), "`edge` \"type\""});
  });

  const std::vector<Query> expected = {
      kCreateInternalIndex,
      {"CREATE (u:__mg_vertex__ {__mg_id__: 1});", {}},
      {"CREATE (u:__mg_vertex__ {__mg_id__: 2});", {}},
      {"MATCH (u:__mg_vertex__), (v:__mg_vertex__) "
       "WHERE u.__mg_id__ = 1 AND v.__mg_id__ = 2 "
       "CREATE (u)-[:`link`]->(v) RETURN 1;",
       {}},
      {"MATCH (u:__mg_vertex__), (v:__mg_vertex__) "
       "WHERE u.__mg_id__ = 2 AND v.__mg_id__ = 1 "
       "CREATE (u)-[:`edge` {`prop1`: $param0, `prop2`: $param1}]->(v) "
       "RETURN 1;",
       {{"param0", mg::Value()}, {"param1", std::vector<mg::Value>{1, 2}}}},
      {"MATCH (u:__mg_vertex__), (v:__mg_vertex__) "
       "WHERE u.__mg_id__ = 1 AND v.__mg_id__ = 1 "
       "CREATE (u)-[:```edge`` \"type\"`]->(v) RETURN 1;",
       {}},
      kDropInternalIndex,
      kRemoveInternalLabelProperty};

  ASSERT_EQ(queries, expected);
}

TEST(MemgraphDestination, CreateIndex) {
  auto queries = Execute([](MemgraphDestination *dest) {
    dest->CreateLabelIndex("label1");
    dest->CreateLabelPropertyIndex("label1", "prop2");
    dest->CreateLabelPropertyIndex("label `1`", "prop 3");
  });

  const std::vector<Query> expected = {
      {"CREATE INDEX ON :`label1`;", {}},
      {"CREATE INDEX ON :`label1`(`prop2`);", {}},
      {"CREATE INDEX ON :`label ``1```(`prop 3`);", {}}};

  ASSERT_EQ(queries, expected);
}

TEST(MemgraphDestination, CreateConstraints) {
  auto queries = Execute([](MemgraphDestination *dest) {
    dest->CreateExistenceConstraint("label`1`", "prop1");
    dest->CreateUniqueConstraint("label1", {"prop 1"});
    dest->CreateUniqueConstraint("label1", {"prop1", "prop2"});
  });

  const std::vector<Query> expected = {
      {"CREATE CONSTRAINT ON (u:`label``1```) ASSERT EXISTS (u.`prop1`);", {}},
      {"CREATE CONSTRAINT ON (u:`label1`) ASSERT u.`prop 1` IS UNIQUE;", {}},
      {"CREATE CONSTRAINT ON (u:`label1`) ASSERT u.`prop1`, u.`prop2` IS "
       "UNIQUE;",
       {}}};

  ASSERT_EQ(queries, expected);
}
