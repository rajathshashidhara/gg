#ifndef SIMPLEDB_HH
#define SIMPLEDB_HH

#include <string>
#include <vector>
#include <functional>

#include "net/requests.hh"
#include "net/address.hh"

struct SimpleDBClientConfig
{
    unsigned num_ { 0 };
    std::vector<Address> address_;

    size_t max_threads { 4 };
    size_t max_batch_size { 32 };

    SimpleDBClientConfig(): address_(0) {}
};

class SimpleDB
{
private:
    SimpleDBClientConfig config_;

public:
    SimpleDB(const SimpleDBClientConfig& config)
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

#endif /* SIMPLEDB_HH */