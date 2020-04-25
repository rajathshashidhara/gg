#ifndef STORAGE_BACKEND_SIMPLEDB_HH
#define STORAGE_BACKEND_SIMPLEDB_HH

#include "storage/backend.hh"
#include "net/simpledb.hh"

class SimpleDBStorageBackend : public StorageBackend
{
private:
    SimpleDB client_;

public:
    SimpleDBStorageBackend(SimpleDBClientConfig& config)
        : client_(config)
    {}

    void put(const std::vector<storage::PutRequest>& requests,
            const PutCallback & success_callback
                = [](const storage::PutRequest&){}) override;

    void get(const std::vector<storage::GetRequest>& requests,
            const GetCallback & success_callback 
                = [](const storage::GetRequest&){}) override;
};

#endif /* STORAGE_BACKEND_SIMPLEDB_HH */