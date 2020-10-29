/* -*-mode:c++; tab-width: 2; indent-tabs-mode: nil; c-basic-offset: 2 -*- */

#ifndef STORAGE_BACKEND_GG_HH
#define STORAGE_BACKEND_GG_HH

#include "backend.hh"
#include "net/gg_objectstore.hh"

class GGStorageBackend : public StorageBackend
{
private:
  GGObjectStoreClient client_;

public:
  GGStorageBackend(GGObjectStoreConfig& config)
    : client_(config) {}

  void put(const std::vector<storage::PutRequest>& requests,
          const PutCallback & success_callback
              = [](const storage::PutRequest&){}) override;

  void get(const std::vector<storage::GetRequest>& requests,
          const GetCallback & success_callback
              = [](const storage::GetRequest&){}) override;
};

#endif /* STORAGE_BACKEND_GG_HH */