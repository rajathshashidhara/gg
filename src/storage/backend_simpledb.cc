#include "backend_simpledb.hh"

using namespace std;
using namespace storage;

void SimpleDBStorageBackend::put(
            const std::vector<storage::PutRequest>& requests,
            const PutCallback & success_callback)
{
    client_.upload_files(requests, success_callback);
}

void SimpleDBStorageBackend::get(
            const std::vector<storage::GetRequest>& requests,
            const GetCallback & success_callback)
{
    client_.download_files(requests, success_callback);
}
