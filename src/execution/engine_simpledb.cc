#include <string>
#include <exception>
#include <iostream>

#include "execution/engine_simpledb.hh"
#include "execution/response.hh"
#include "protobufs/gg.pb.h"
#include "protobufs/netformats.pb.h"
#include "protobufs/util.hh"
#include "thunk/ggutils.hh"
#include "thunk/thunk_writer.hh"
#include "util/iterator.hh"
#include "util/crc16.hh"

using namespace std;
using namespace gg;
using namespace gg::thunk;
using namespace simpledb::proto;

class ProgramFinished : public exception {};

void SimpleDBExecutionEngine::init(ExecutionLoop& loop)
{
    for (size_t idx = 0; idx < address_.size(); idx++)
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
                        auto response = this->workers_[idx].parser.front();
                        this->workers_[idx].parser.pop();
                        auto &thunk = this->workers_[idx].executing_thunks[response.id()].get();

                        if (response.return_code() != 0)
                        {
                            failure_callback_(thunk.hash(), JobStatus::ExecutionFailure);
                            return true;
                        }

                        gg::protobuf::ExecutionResponse exec_resp;
                        protoutil::from_string(response.val(), exec_resp);
                        std::cerr << exec_resp.stdout() << endl;
                        if (JobStatus::Success != static_cast<JobStatus>(exec_resp.return_code()))
                        {
                            failure_callback_(thunk.hash(), static_cast<JobStatus>(exec_resp.return_code()));
                            return true;
                        }

                        for (const auto& executed_thunk: exec_resp.executed_thunks())
                        {
                            for ( const auto & output : executed_thunk.outputs()) {
                                gg::cache::insert(gg::hash::for_output(executed_thunk.thunk_hash(), output.tag()), output.hash());

                                if (output.data().length() > 0)
                                {
                                    roost::atomic_create(output.data(),
                                                        gg::paths::blob(output.hash()));
                                }
                            }

                            gg::cache::insert(executed_thunk.thunk_hash(), executed_thunk.outputs(0).hash());

                            vector<ThunkOutput> thunk_outputs;
                            for (auto & output : executed_thunk.outputs()) {
                                if (crc16(output.hash()) % this->workers_.size() != idx)
                                    this->workers_[idx].objects.insert(output.hash());
                                thunk_outputs.emplace_back(move(output.hash()), move(output.tag()));
                            }

                            success_callback_(executed_thunk.thunk_hash(), move(thunk_outputs), 0);
                        }

                        break;
                    }

                    workers_.at(idx).scheduled_jobs_--;
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

        workers_.emplace_back(idx, max(1ul, max_jobs_/address_.size()), move(connection));
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
                        KVRequest& request,
                        const SelectionStrategy s)
{
    // static const bool timelog = ( getenv( "GG_TIMELOG" ) != nullptr );
    static string exec_func_ = "gg-execute-simpledb-static";

    request.set_id(finished_jobs_++);
    auto exec_request = request.mutable_exec_request();
    exec_request->set_func(exec_func_);

    // exec_request->add_immediate_args("--cleanup");
    // if (timelog)
    //     exec_request->add_immediate_args("--timelog");
    exec_request->add_immediate_args(thunk.hash());
    exec_request->add_immediate_args(ThunkWriter::serialize(thunk));

    for (const auto & item : join_containers(thunk.values(), thunk.executables()))
    {
        exec_request->add_file_args(item.first);
    }

    if (free_workers_.size() == 0)
        throw runtime_error("No free workers to pick from.");

    size_t selected_worker = numeric_limits<size_t>::max();
    switch (s)
    {
        case SelectionStrategy::First:
            selected_worker = *free_workers_.begin();
            break;

        case SelectionStrategy::MostObjectsWeight:
        {
            size_t max_common_size = 0;

            unordered_set<string> thunk_objects;
            for ( const auto & item : join_containers(thunk.values(), thunk.executables())) {
                thunk_objects.insert( item.first );
            }

            for (const auto & free_lambda : free_workers_) {
                const auto &worker = workers_.at( free_lambda );
                size_t common_size = 0;

                for ( const string & obj : thunk_objects ) {
                    if (crc16(obj) % workers_.size() == free_lambda)
                    {
                        common_size += gg::hash::size(obj);
                    }
                    else if (worker.objects.count( obj ) ) {
                        common_size += gg::hash::size(obj);
                    }
                }

                if ( common_size > max_common_size ) {
                    selected_worker = free_lambda;
                    max_common_size = common_size;
                }
            }

            if (selected_worker == numeric_limits<size_t>::max()) {
                selected_worker = *free_workers_.begin();
            }
            break;
        }

        case SelectionStrategy::MostObjects:
        {
            size_t max_common_size = 0;

            unordered_set<string> thunk_objects;
            for ( const auto & item : join_containers(thunk.values(), thunk.executables())) {
                thunk_objects.insert( item.first );
            }

            for (const auto & free_lambda : free_workers_) {
                const auto &worker = workers_.at( free_lambda );
                size_t common_size = 0;

                for ( const string & obj : thunk_objects ) {
                    if (crc16(obj) % workers_.size() == free_lambda)
                    {
                        common_size += 1;
                    }
                    else if (worker.objects.count( obj ) ) {
                        common_size += 1;
                    }
                }

                if ( common_size > max_common_size ) {
                    selected_worker = free_lambda;
                    max_common_size = common_size;
                }
            }

            if (selected_worker == numeric_limits<size_t>::max()) {
                selected_worker = *free_workers_.begin();
            }
            break;
        }

        case SelectionStrategy::Random:
        {
            // Not yet implemented!
            selected_worker = *free_workers_.begin();
            break;
        }

        case SelectionStrategy::LargestObject:
        {
            /* what's the largest object in this thunk? */
            string largest_hash;
            uint32_t largest_size = 0;

            for ( const auto & item : join_containers( thunk.values(), thunk.executables() ) ) {
                if ( gg::hash::size( item.first ) > largest_size ) {
                    largest_size = gg::hash::size( item.first );
                    largest_hash = item.first;
                }
            }

            if ( largest_hash.length() ) {
                for ( const auto & free_worker : free_workers_ ) {
                    if ( workers_.at( free_worker ).objects.count( largest_hash ) ) {
                        selected_worker = free_worker;
                        break;
                    }
                }
            }

            if (selected_worker == numeric_limits<size_t>::max()) {
                selected_worker = *free_workers_.begin();
            }
            break;
        }

        default:
            throw runtime_error( "invalid selection strategy" );
    }

    for (const auto & item : join_containers(thunk.values(), thunk.executables()))
    {
        workers_[selected_worker].objects.insert(item.first);
    }

    return selected_worker;
}

void SimpleDBExecutionEngine::force_thunk(const Thunk& thunk, ExecutionLoop & loop)
{
    (void) loop;

    KVRequest request;
    const size_t worker_idx = prepare_worker(thunk, request);
    const size_t slot = workers_[worker_idx].idx;
    workers_[worker_idx].idx++;
    if (workers_[worker_idx].idx >= workers_[worker_idx].num_pipeline)
        workers_[worker_idx].idx = 0;
    workers_[worker_idx].scheduled_jobs_++;
    workers_[worker_idx].executing_thunks[slot].reset(thunk);

    running_jobs_++;
    if (workers_[worker_idx].scheduled_jobs_ == workers_[worker_idx].num_pipeline)
    {
        workers_[worker_idx].state = State::Busy;
        free_workers_.erase(worker_idx);
    }

    string s_request_(sizeof(size_t), 0);
    *((size_t*) &s_request_[0]) = request.ByteSize();
    s_request_.append(request.SerializeAsString());
    workers_[worker_idx].connection->enqueue_write(s_request_);
}