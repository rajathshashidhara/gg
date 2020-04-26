#include <string>
#include <iostream>

#include "engine_baseline.hh"
#include "response.hh"
#include "protobufs/gg.pb.h"


using namespace std;
using namespace gg;
using namespace gg::thunk;

ExecutionRequest BaselineExecutionEngine::generate_request(const Thunk& thunk)
{
    static const bool timelog = (getenv("GG_TIMELOG") != nullptr);

    ExecutionRequest request;

    *request.add_thunks() = Thunk::execution_request(thunk);
    request.set_storage_backend(gg::remote::storage_backend_uri());
    request.set_timelog(timelog);

    return request;
}

void BaselineExecutionEngine::force_thunk(const Thunk& thunk,
                                ExecutionLoop& exec_loop)
{
    ExecutionRequest request = move(generate_request(thunk));
    uint64_t connection_id = exec_loop.make_exec_request<TCPConnection>(
        thunk.hash(), address_, request,
        [this] (const uint64_t id, const string & thunk_hash,
            const gg::protobuf::ExecutionResponse & exec_response) -> bool
        {
            running_jobs_--;

            ExecutionResponse response = ExecutionResponse::parse_message(exec_response);
            /* print the output, if there's any */
            if ( response.stdout.length() ) {
                cerr << response.stdout << endl;
            }

            switch ( response.status ) {
            case JobStatus::Success:
            {
                if ( response.thunk_hash != thunk_hash ) {
                cerr << http_response.str() << endl;
                throw runtime_error( "expected output for " +
                                    thunk_hash + ", got output for " +
                                    response.thunk_hash );
                }

                for ( const auto & output : response.outputs ) {
                gg::cache::insert( gg::hash::for_output( response.thunk_hash, output.tag ), output.hash );

                if ( output.data.length() ) {
                    roost::atomic_create( base64::decode( output.data ),
                                        gg::paths::blob( output.hash ) );
                }
                }

                gg::cache::insert( response.thunk_hash, response.outputs.at( 0 ).hash );

                vector<ThunkOutput> thunk_outputs;
                for ( auto & output : response.outputs ) {
                thunk_outputs.emplace_back( move( output.hash ), move( output.tag ) );
                }

                success_callback_( response.thunk_hash, move( thunk_outputs ), 0);

                start_times_.erase( id );
                break;
            }

            default: /* in case of any other failure */
                failure_callback_( thunk_hash, response.status );
            }

            return false;
        },
        [this] ( const uint64_t id, const string & thunk_hash )
        {
        start_times_.erase( id );
        failure_callback_( thunk_hash, JobStatus::SocketFailure );
        }
    );

    start_times_.insert( { connection_id, chrono::steady_clock::now() } );

    running_jobs_++;
}

size_t BaselineExecutionEngine::job_count() const
{
  return running_jobs_;
}

bool BaselineExecutionEngine::can_execute( const gg::thunk::Thunk & thunk ) const
{
  return true;
}
