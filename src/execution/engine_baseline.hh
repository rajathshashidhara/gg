#ifndef ENGINE_BASELINE_HH
#define ENGINE_BASELINE_HH

#include <string>
#include <vector>
#include <unordered_map>
#include <chrono>

#include "engine.hh"
#include "net/address.hh"
#include "thunk/thunk.hh"
#include "protobufs/gg.pb.h"

class BaselineExecutionEngine : public ExecutionEngine
{
private:
    enum class State { Idle, Busy };
    std::vector<Address> address_;
    std::vector<State> worker_state_;
    size_t running_jobs_ {0};
    std::map<uint64_t, std::chrono::steady_clock::time_point> start_times_ {};

    gg::protobuf::ExecutionRequest
            generate_request(const gg::thunk::Thunk& thunk);

public:
    BaselineExecutionEngine(const size_t max_jobs,
                const std::vector<Address>& workers)
        : ExecutionEngine(max_jobs), address_(workers),
            worker_state_(max_jobs, State::Idle) {}

    void force_thunk(const gg::thunk::Thunk& thunk,
            ExecutionLoop& exec_loop) override;
    bool is_remote() const { return true; }
    bool can_execute(const gg::thunk::Thunk& thunk) const override;
    std::string label() const override { return "\u0fff"; }
    size_t job_count() const override;
};

#endif /* ENGINE_BASELINE_HH */
