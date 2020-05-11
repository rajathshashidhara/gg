#ifndef ENGINE_SIMPLEDB_HH
#define ENGINE_SIMPLEDB_HH

#include <string>
#include <memory>
#include <vector>
#include <unordered_set>

#include "engine.hh"
#include "net/address.hh"
#include "net/socket.hh"
#include "net/protobuf_stream_parser.hh"
#include "thunk/thunk.hh"
#include "protobufs/netformats.pb.h"

class SimpleDBExecutionEngine : public ExecutionEngine
{
private:
    enum class State { Idle, Busy };
    std::vector<Address> address_;

    struct Worker
    {
        size_t id;
        const size_t num_pipeline;
        size_t scheduled_jobs_ { 0 };
        size_t idx {0};
        State state { State::Idle };
        std::shared_ptr<TCPConnection> connection;
        std::unordered_set<std::string> objects {};
        std::vector<Optional<gg::thunk::Thunk>> executing_thunks;
        ProtobufStreamParser<simpledb::proto::KVResponse> parser {};

        Worker(const size_t id, const size_t num_pipeline,
                        std::shared_ptr<TCPConnection> && connection)
            : id(id), num_pipeline(num_pipeline),
            connection(std::move(connection)), executing_thunks(num_pipeline) {}
    };

    enum class SelectionStrategy
    {
    First, Random, MostObjects, MostObjectsWeight, LargestObject,
    };

    uint64_t finished_jobs_ { 0 };
    size_t running_jobs_ { 0 };
    std::vector<Worker> workers_ {};
    std::set<size_t> free_workers_ {};

    size_t prepare_worker(const gg::thunk::Thunk& thunk,
            simpledb::proto::KVRequest& exec_request,
            const SelectionStrategy s = SelectionStrategy::First);

public:
    SimpleDBExecutionEngine(const size_t max_jobs,
            const std::vector<Address>& workers_addr)
        : ExecutionEngine(max_jobs), address_(workers_addr) {}
    
    void init(ExecutionLoop& loop) override;
    void force_thunk(const gg::thunk::Thunk& thunk,
            ExecutionLoop& exec_loop) override;
    bool is_remote() const { return true; }
    bool can_execute(const gg::thunk::Thunk& thunk) const override;
    std::string label() const override { return "simpledb"; }
    size_t job_count() const override;
};

#endif /* ENGINE_SIMPLEDB_HH */