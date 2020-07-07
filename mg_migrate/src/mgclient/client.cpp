#include "mgclient/client.hpp"

#include <optional>

#include <glog/logging.h>
#include <mgclient.h>

namespace mg {

std::unique_ptr<Client> Client::Connect(const Client::Params &params) {
  mg_session_params *mg_params = mg_session_params_make();
  if (!mg_params) {
    LOG(ERROR) << "Failed to allocate session params.";
    return nullptr;
  }
  mg_session_params_set_host(mg_params, params.host.c_str());
  mg_session_params_set_port(mg_params, params.port);
  if (!params.username.empty()) {
    mg_session_params_set_username(mg_params, params.username.c_str());
    mg_session_params_set_password(mg_params, params.password.c_str());
  }
  mg_session_params_set_client_name(mg_params, params.client_name.c_str());
  mg_session_params_set_sslmode(
      mg_params, params.use_ssl ? MG_SSLMODE_REQUIRE : MG_SSLMODE_DISABLE);

  mg_session *session = nullptr;
  int status = mg_connect(mg_params, &session);
  mg_session_params_destroy(mg_params);
  if (status < 0) {
    return nullptr;
  }

  // Using `new` to access private constructor.
  return std::unique_ptr<Client>(new Client(session));
}

Client::Client(mg_session *session) : session_(session) {}

Client::~Client() { mg_session_destroy(session_); }

bool Client::Execute(const std::string &statement) {
  int status = mg_session_run(session_, statement.c_str(), nullptr, nullptr);
  if (status < 0) {
    LOG(ERROR) << "Execution failed: " << mg_session_error(session_);
    return false;
  }
  return true;
}

bool Client::Execute(const std::string &statement, const ConstMap &params) {
  int status =
      mg_session_run(session_, statement.c_str(), params.ptr(), nullptr);
  if (status < 0) {
    LOG(ERROR) << "Execution failed: " << mg_session_error(session_);
    return false;
  }
  return true;
}

std::optional<std::vector<Value>> Client::FetchOne() {
  mg_result *result;
  int status = mg_session_pull(session_, &result);
  if (status != 1) {
    return std::nullopt;
  }

  std::vector<Value> values;
  const mg_list *list = mg_result_row(result);
  const size_t list_length = mg_list_size(list);
  values.reserve(list_length);
  for (size_t i = 0; i < list_length; ++i) {
    values.emplace_back(Value(mg_list_at(list, i)));
  }
  return values;
}

}  // namespace mg
