/* -*-mode:c++; tab-width: 2; indent-tabs-mode: nil; c-basic-offset: 2 -*- */

#include "backend_gg.hh"

using namespace std;
using namespace storage;

void GGStorageBackend::put(
            const std::vector<storage::PutRequest>& requests,
            const PutCallback & success_callback)
{
    client_.upload_files(requests, success_callback);
}

void GGStorageBackend::get(
            const std::vector<storage::GetRequest>& requests,
            const GetCallback & success_callback)
{
    client_.download_files(requests, success_callback);
}
