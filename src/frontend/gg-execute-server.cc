/* -*-mode:c++; tab-width: 2; indent-tabs-mode: nil; c-basic-offset: 2 -*- */

#include <string>
#include <iostream>
#include <stdexcept>
#include <cstdlib>
#include <map>
#include <list>
#include <deque>
#include <unordered_map>

#include "protobufs/gg.pb.h"
#include "protobufs/util.hh"
#include "net/address.hh"
#include "net/http_request.hh"
#include "net/http_response.hh"
#include "net/http_request_parser.hh"
#include "execution/loop.hh"
#include "storage/backend.hh"
#include "thunk/ggutils.hh"
#include "thunk/thunk.hh"
#include "thunk/thunk_reader.hh"
#include "util/system_runner.hh"
#include "util/path.hh"
#include "util/base64.hh"
#include "util/pipe.hh"
#include "util/iterator.hh"
#include "util/lru.hh"
#include "util/optional.hh"
#include "util/timelog.hh"

using namespace std;
using namespace std::chrono;
using namespace gg;

string get_canned_response( const int status, const HTTPRequest & request )
{
  const static map<int, string> status_messages = {
    { 200, "OK" },
    { 400, "Bad Request" },
    { 404, "Not Found" },
    { 405, "Method Not Allowed" },
  };

  HTTPResponse response;
  response.set_request( request );
  response.set_first_line( "HTTP/1.1 " + to_string( status ) + " " + status_messages.at( status ) );
  response.add_header( HTTPHeader{ "Content-Length", "0" } );
  response.add_header( HTTPHeader{ "Content-Type", "text/plain" } );
  response.done_with_headers();
  response.read_in_body( "" );
  assert( response.state() == COMPLETE );

  return response.str();
}

void usage( char * argv0 )
{
  cerr << "Usage: " << argv0 << " IP PORT [CACHE-SIZE]" << endl;
}

struct ExecutionInfo {
  weak_ptr<TCPConnection> connection;
  const HTTPRequest http_request;
  const protobuf::ExecutionRequest exec_request;
  Optional<TimeLog> timelog { };
  string output { "" };
  int status { 0 };

  ExecutionInfo(weak_ptr<TCPConnection> connection,
    const HTTPRequest&& http_request,
    const protobuf::ExecutionRequest&& exec_request): connection(connection),
      http_request(http_request), exec_request(exec_request) {}
};

int main( int argc, char * argv[] )
{
  try {
    if ( argc <= 0 ) {
      abort();
    }

    if ( argc < 3 ) {
      usage( argv[ 0 ] );
      return EXIT_FAILURE;
    }

    /* make sure that .gg directory exists */
    gg::paths::blobs();

    int port_argv = stoi( argv[ 2 ] );

    if ( port_argv <= 0 or port_argv > numeric_limits<uint16_t>::max() ) {
      throw runtime_error( "invalid port" );
    }

    Address listen_addr { argv[ 1 ], static_cast<uint16_t>( port_argv ) };

    size_t cache_size = std::numeric_limits<size_t>::max();

    if (argc == 4) {
      cache_size = (size_t) atol(argv[3]);
    }

    auto cache = make_shared<LRU>(cache_size);
    ExecutionLoop loop;
    deque<shared_ptr<ExecutionInfo>> get_queue;
    deque<shared_ptr<ExecutionInfo>> exec_queue;
    deque<shared_ptr<ExecutionInfo>> put_queue;

    loop.make_listener( listen_addr,
      [&get_queue] ( ExecutionLoop & loop, TCPSocket && socket ) {
        /* an incoming connection! */

        auto request_parser = make_shared<HTTPRequestParser>();

        loop.add_connection<TCPSocket>( move( socket ),
          [request_parser, &get_queue] ( shared_ptr<TCPConnection> connection, string && data ) {
            request_parser->parse( move( data ) );

            while ( not request_parser->empty() ) {
              HTTPRequest http_request { move( request_parser->front() ) };
              request_parser->pop();

              const static string reset_line { "GET /reset HTTP/1.1" };
              cerr << http_request.first_line() << endl;
              if ( http_request.first_line().compare( 0, reset_line.length(), reset_line ) == 0 ) {
                /* the user wants us to clean up the .gg directory */
                roost::empty_directory( gg::paths::blobs() );
                roost::empty_directory( gg::paths::reductions() );
                // XXX roost::empty_directory( gg::paths::remotes() );
                cerr << "cleared" << endl;

                connection->enqueue_write( get_canned_response( 200, http_request ) );
                continue;
              }

              protobuf::ExecutionRequest exec_request;

              try {
                protoutil::from_json( http_request.body(), exec_request );
              }
              catch (...) {
                connection->enqueue_write( get_canned_response( 400, http_request ) );
                continue;
              }

              shared_ptr<ExecutionInfo> exec_info = make_shared<ExecutionInfo>(weak_ptr<TCPConnection>(connection),
                                move(http_request), move(exec_request));
              exec_info->timelog.reset(); /*> Reset timelog */
              get_queue.push_back(exec_info);
            }

            return true;
          },
          [] () {
            cerr << "error" << endl;
          },
          [] () {
            cerr << "closed" << endl;
          }
        );

        return true;
      }
    );

    std::chrono::milliseconds stall_start;
    bool in_exec, in_get, in_put, in_stall;
    in_exec = in_get = in_put = in_stall = false;

    while ( true ) {
      loop.loop_once( -1 );

      /* Enter stall if! */
      /* 1. Not already in stall! */
      /* 2. Get in-progress or queued */
      /* 3. No execution in-progress or queued */
      if (!in_stall && (in_get || get_queue.size() > 0) && (!in_exec && exec_queue.size() == 0)) {
        in_stall = true;
        stall_start = duration_cast<milliseconds>(system_clock::now().time_since_epoch());
      }

      /* GET dependencies! */
      if (get_queue.size() > 0) {
        shared_ptr<ExecutionInfo> get_info = get_queue.front();
        exec_queue.pop_front();

        get_info->timelog->add_point("begin_prefetch");

        /* GET phase */
        cache->cleanup(true);
        loop.add_child_process( "thunk-get",
          [get_info, cache, &exec_queue, &in_get]
          ( const uint64_t, const string &, const int ) { /* success callback */

            /* Add dependencies to cache */
            for (auto & request_item : get_info->exec_request.thunks()) {
              const string & hash = request_item.hash();
              gg::thunk::Thunk thunk { ThunkReader::read(paths::blob(hash), hash) };
              cache->access(thunk.hash(), true);
              for (auto& dep : join_containers(thunk.values(), thunk.executables())) {
                cache->access(dep.first, true);
              }
            }

            get_info->timelog->add_point("prefetch");

            in_get = false;

            /* Move request to exec queue */
            exec_queue.push_back(get_info);
          },
          [exec_request=get_info->exec_request] () -> int { /* child process */
            setenv( "GG_STORAGE_URI", exec_request.storage_backend().c_str(), true );

            for ( auto & request_item : exec_request.thunks() ) {
              roost::atomic_create( base64::decode( request_item.data() ),
                                    paths::blob( request_item.hash() ) );
            }

            vector<storage::GetRequest> download_items;
            for ( auto & request_item : exec_request.thunks() ) {
              const string & hash = request_item.hash();
              gg::thunk::Thunk thunk { ThunkReader::read(paths::blob(hash), hash) };
              bool executables = false;

              auto check_dep =
                [&download_items, &executables]( const gg::thunk::Thunk::DataItem & item ) -> void
                {
                  const auto target_path = gg::paths::blob( item.first );

                  if ( not roost::exists( target_path )
                      or roost::file_size( target_path ) != gg::hash::size( item.first ) ) {
                    if ( executables ) {
                      download_items.push_back( { item.first, target_path, 0544 } );
                    }
                    else {
                      download_items.push_back( { item.first, target_path, 0444 } );
                    }
                  }
                };

              for_each( thunk.values().cbegin(), thunk.values().cend(),
                        check_dep );

              executables = true;
              for_each( thunk.executables().cbegin(), thunk.executables().cend(),
                        check_dep );
            }

            if ( download_items.size() > 0 ) {
              auto storage_backend = StorageBackend::create_backend( gg::remote::storage_backend_uri() );
              storage_backend->get( download_items );
            }

            return 0;
          },
          false
        );
        in_get = true;
      }

      /* Launch new execution! */
      if (!in_exec && exec_queue.size() > 0) {
        shared_ptr<ExecutionInfo> exec_info = exec_queue.front();
        exec_queue.pop_front();

        exec_info->timelog->add_point("begin_thunk");

        int pipe_fds[ 2 ];
        CheckSystemCall( "pipe", pipe( pipe_fds ) );

        loop.add_child_process(
          "thunk-execution",
          [exec_info, cache, pipe_fds, &in_exec, &put_queue]
          ( const uint64_t, const string &, const int status ) { /* success callback */
            pair<FileDescriptor, FileDescriptor> pipe { pipe_fds[0], pipe_fds[1] };

            string output;
            pipe.second.close();
            while ( not pipe.first.eof() ) {
              output.append(pipe.first.read());
            }

            /* Unpin the dependencies from cache! */
            for (auto & request_item : exec_info->exec_request.thunks()) {
              const string & hash = request_item.hash();
              gg::thunk::Thunk thunk { ThunkReader::read(paths::blob(hash), hash) };
              cache->unpin(thunk.hash());
              for (auto& dep : join_containers(thunk.values(), thunk.executables())) {
                cache->unpin(dep.first);
              }
            }

            exec_info->output = output;
            exec_info->status = status;
            in_exec = false;

            exec_info->timelog->add_point("thunk");

            /* Move request to exec queue */
            put_queue.push_back(exec_info);
          },
          [pipe_out_fd=pipe_fds[1], exec_request=exec_info->exec_request] () -> int {  /* child process */
            setenv( "GG_STORAGE_URI", exec_request.storage_backend().c_str(), true );

            vector<string> command {
              "gg-execute-static",
              "--get-dependencies",
              // "--put-output",
              "--timelog",
              // "--cleanup"
            };

            for ( auto & request_item : exec_request.thunks() ) {
              command.emplace_back( request_item.hash() );
            }

            CheckSystemCall( "dup2", dup2( pipe_out_fd, STDOUT_FILENO ) );

            return ezexec( command[ 0 ], command, {}, true, true );
          },
          false
        );

        if (in_stall) {
          in_stall = false;
          auto now = duration_cast<milliseconds>(system_clock::now().time_since_epoch());
          cerr << "stall " << (now - stall_start).count() << endl;
        }
        in_exec = true;
      }

      /* PUT output! */
      if (put_queue.size() > 0) {
        shared_ptr<ExecutionInfo> put_info = put_queue.front();
        put_queue.pop_front();

        put_info->timelog->add_point("begin_upload");

        loop.add_child_process(
          "thunk-put",
          [put_info, cache, &in_put]
          ( const uint64_t, const string &, const int ) { /* success callback */
            in_put = false;

            protobuf::ExecutionResponse response;

            for ( auto & request_item : put_info->exec_request.thunks() ) {
              protobuf::ResponseItem execution_response;
              execution_response.set_thunk_hash( request_item.hash() );

              bool discard_rest = false;
              for ( const auto & tag : request_item.outputs() ) {
                protobuf::OutputItem output_item;
                Optional<cache::ReductionResult> result = cache::check( gg::hash::for_output( request_item.hash(), tag ) );

                if ( not result.initialized() ) {
                  discard_rest = true;
                  break;
                }

                const auto output_path = paths::blob( result->hash );
                const string output_data = result->hash[ 0 ] == 'T'
                                          ? base64::encode( roost::read_file( output_path ) )
                                          : "";

                cache->access(result->hash);

                output_item.set_tag( tag );
                output_item.set_hash( result->hash );
                output_item.set_size( roost::file_size( output_path ) );
                output_item.set_executable( roost::is_executable( output_path ) );
                output_item.set_data( output_data );

                *execution_response.add_outputs() = output_item;
              }

              if ( discard_rest ) { break; }
              *response.add_executed_thunks() = execution_response;
            }

            cache->cleanup(true);

            if (put_info->connection.expired()) {
              return;
            }

            put_info->timelog->add_point("upload");

            response.set_return_code( put_info->status );
            response.set_stdout( put_info->output + put_info->timelog->str());

            const string response_json = protoutil::to_json( response );

            HTTPResponse http_response;
            http_response.set_request( put_info->http_request );
            http_response.set_first_line( "HTTP/1.1 200 OK" );
            http_response.add_header( HTTPHeader{ "Content-Length", to_string( response_json.size() ) } );
            http_response.add_header( HTTPHeader{ "Content-Type", "application/octet-stream" } );
            http_response.done_with_headers();
            http_response.read_in_body( response_json );
            assert( http_response.state() == COMPLETE );

            auto conn = put_info->connection.lock();
            conn->enqueue_write( http_response.str() );
          },
          [exec_request=put_info->exec_request] () -> int {  /* child process */
            setenv( "GG_STORAGE_URI", exec_request.storage_backend().c_str(), true );
            vector<storage::PutRequest> requests;

            for ( auto & request_item : exec_request.thunks() ) {
              for ( const auto & tag : request_item.outputs() ) {
                protobuf::OutputItem output_item;
                Optional<cache::ReductionResult> result = cache::check( gg::hash::for_output( request_item.hash(), tag ) );

                requests.push_back( { gg::paths::blob( result->hash ), result->hash,
                                      gg::hash::to_hex( result->hash ) } );
              }
            }

            if (requests.size() > 0) {
              auto storage_backend = StorageBackend::create_backend( gg::remote::storage_backend_uri() );
              storage_backend->put( requests );
            }

            return 0;
          },
          false
        );
        in_put = true;
      }
    }
  }
  catch ( const exception &  e ) {
    print_exception( argv[ 0 ], e );
    return EXIT_FAILURE;
  }

  return EXIT_SUCCESS;
}
