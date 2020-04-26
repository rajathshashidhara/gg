#ifndef ENGINE_BASELINE_HH
#define ENGINE_BASELINE_HH

#include <string>
#include <unordered_map>
#include <chrono>

#include "engine.hh"
#include "net/address.hh"
#include "thunk/thunk.hh"

class BaselineExecutionEngine : public ExecutionEngine
{
private:
    Address address_;

    size_t running_jobs_ {0};
    std::map<uint64_t, std::chrono::steady_clock::time_point> start_times_ {};

    std::string generate_request(const gg::thunk::Thunk& thunk);

public:
    BaselineExecutionEngine(const size_t max_jobs,
            const std:string& ip,
            const uint16_t port)
        : ExecutionEngine(max_jobs), address_(ip, port) {}
    
    void force_thunk(const gg::thunk::Thunk& thunk,
            ExecutionLoop& exec_loop) override;
    bool is_remote() const { return true; }
    bool can_execute(const gg::thunk::Thunk& thunk) const override;
    std::string label() const override { return "\u0fff"; }
    size_t job_count() const override;
};

#endif /* ENGINE_BASELINE_HH */