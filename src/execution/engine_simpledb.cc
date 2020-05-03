#include <string>
#include <exception>
#include <iostream>

#include "execution/engine_simpledb.hh"
#include "execution/response.hh"
#include "protobufs/gg.pb.h"
#include "protobufs/netformats.pb.h"
#include "protobufs/util.hh"
#include "thunk/ggutils.hh"
#include "util/iterator.hh"

using namespace std;
using namespace gg;
using namespace gg::thunk;
using namespace simpledb::proto;

class ProgramFinished : public exception {};

void SimpleDBExecutionEngine::init(ExecutionLoop& loop)
{
    for (size_t idx = 0; idx < max_jobs_; idx++)
    {
        shared_ptr<TCPConnection> connection =
            loop.make_connection<TCPConnection>(
                address_[idx],
                [this, idx](shared_ptr<TCPConnection>,
                            string && data) -> bool
                {
                    this->workers_[idx].parser.parse(data);
                    while (not this->workers_[idx].parser.empty())
                    {
                        auto &response = this->workers_[idx].parser.front();
                        auto &thunk = this->workers_[idx].executing_thunk.get();

                        if (response.return_code() != 0)
                        {
                            failure_callback_(thunk.hash(), JobStatus::ExecutionFailure);
                            return true;
                        }

                        gg::protobuf::ExecutionResponse exec_resp;
                        protoutil::from_string(response.val(), exec_resp);

                        for (const auto& executed_thunk: exec_resp.executed_thunks())
                        {
                            for ( const auto & output : executed_thunk.outputs()) {
                                gg::cache::insert(gg::hash::for_output(executed_thunk.thunk_hash(), output.tag()), output.hash());
                            }

                            gg::cache::insert(executed_thunk.thunk_hash(), executed_thunk.outputs(0).hash());

                            vector<ThunkOutput> thunk_outputs;
                            for (auto & output : executed_thunk.outputs()) {
                                thunk_outputs.emplace_back(move(output.hash()), move(output.tag()));
                            }

                            success_callback_(executed_thunk.thunk_hash(), move(thunk_outputs), 0);
                        }

                        break;
                    }

                    workers_.at(idx).state = State::Idle;
                    free_workers_.insert(idx);
                    running_jobs_--;

                    return true;
                },
                [] () {
                    throw ProgramFinished();
                },
                [] () {
                    throw ProgramFinished();
                }
        );

        workers_.emplace_back(idx, move(connection));
        free_workers_.insert(idx);
    }
}

size_t SimpleDBExecutionEngine::job_count() const
{
  return running_jobs_;
}

bool SimpleDBExecutionEngine::can_execute( const Thunk & thunk ) const
{
  (void) thunk;
  return true;
}

size_t SimpleDBExecutionEngine::prepare_worker(
                        const Thunk& thunk,
                        KVRequest& request)
{
    static const bool timelog = ( getenv( "GG_TIMELOG" ) != nullptr );
    static string exec_func_ = "gg-execute-simpledb-static";

    request.set_id(finished_jobs_++);
    auto exec_request = request.mutable_exec_request();
    exec_request->set_func(exec_func_);

    exec_request->add_immediate_args("--cleanup");
    if (timelog)
        exec_request->add_immediate_args("--timelog");
    exec_request->add_immediate_args(thunk.hash());

    for (const auto & item : join_containers(thunk.values(), thunk.executables()))
    {
        exec_request->add_file_args(item.first);
    }

    if (free_workers_.size() == 0)
        throw runtime_error("No free workers to pick from.");

    // Add Selection strategies here!
    return *free_workers_.begin();
}

void SimpleDBExecutionEngine::force_thunk(const Thunk& thunk, ExecutionLoop & loop)
{
    (void) loop;

    KVRequest request;
    const size_t worker_idx = prepare_worker(thunk, request);

    running_jobs_++;
    workers_[worker_idx].state = State::Busy;
    workers_[worker_idx].executing_thunk.reset(thunk);
    free_workers_.erase(worker_idx);

    string s_request_(sizeof(size_t), 0);
    *((size_t*) &s_request_[0]) = request.ByteSize();
    s_request_.append(request.SerializeAsString());
    workers_[worker_idx].connection->enqueue_write(s_request_);
}