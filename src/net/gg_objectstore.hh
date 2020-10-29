#ifndef GG_OBJECT_STORE_HH
#define GG_OBJECT_STORE_HH

#include <string>
#include <vector>
#include <functional>

#include "net/requests.hh"
#include "net/address.hh"

struct GGObjectStoreConfig
{
  Address address_;
  size_t max_threads { 4 };
  size_t max_batch_size { 32 };

  GGObjectStoreConfig(const std::string host, uint16_t port): address_(host, port) {}
};

class GGObjectStoreClient
{
private:
  GGObjectStoreConfig config_;

public:
  GGObjectStoreClient(const GGObjectStoreConfig& config)
      : config_(config)
  {}

  void upload_files(
      const std::vector<storage::PutRequest>& upload_requests,
      const std::function<void(const storage::PutRequest&)>& success_callback
                      = [](const storage::PutRequest&){});

  void download_files(
      const std::vector<storage::GetRequest>& download_requests,
      const std::function<void(const storage::GetRequest&)>& success_callback
                          = [](const storage::GetRequest&){});
};

#endif /* GG_OBJECT_STORE_HH */