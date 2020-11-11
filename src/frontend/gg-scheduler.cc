/* -*-mode:c++; tab-width: 2; indent-tabs-mode: nil; c-basic-offset: 2 -*- */
#include <map>
#include <string>
#include <memory>
#include <stdexcept>
#include <iostream>
#include <vector>
#include <queue>
#include <mutex>
#include <chrono>
#include <thread>
#include <tuple>
#include <cstdlib>
#include <getopt.h>
#include <sys/time.h>
#include <sys/resource.h>

#include "execution/scheduler.hh"
#include "net/s3.hh"
#include "storage/backend_local.hh"
#include "storage/backend_s3.hh"
#include "thunk/ggutils.hh"
#include "thunk/placeholder.hh"
#include "thunk/thunk_reader.hh"
#include "thunk/thunk.hh"
#include "execution/engine.hh"
#include "execution/engine_local.hh"
#include "execution/engine_lambda.hh"
#include "execution/engine_baseline.hh"
#include "execution/engine_gg.hh"
#include "execution/engine_meow.hh"
#include "execution/engine_gcloud.hh"
#include "execution/engine_simpledb.hh"
#include "tui/status_bar.hh"
#include "util/digest.hh"
#include "util/exception.hh"
#include "util/optional.hh"
#include "util/path.hh"
#include "util/timeit.hh"
#include "util/util.hh"
#include "util/tokenize.hh"


#include "net/http_request.hh"
#include "net/http_response.hh"
#include "net/http_request_parser.hh"
#include "execution/loop.hh"
#include "thunk/ggutils.hh"
#include "util/path.hh"

using namespace std;
using namespace std::chrono;
using namespace gg::thunk;

constexpr char FORCE_NO_STATUS[] = "GG_FORCE_NO_STATUS";
constexpr char FORCE_DEFAULT_ENGINE[] = "GG_FORCE_DEFAULT_ENGINE";
constexpr char FORCE_MAX_JOBS[] = "GG_FORCE_MAX_JOBS";
constexpr char FORCE_TIMEOUT[] = "GG_FORCE_TIMEOUT";

string get_canned_response( const int status, const HTTPRequest & request )
{
  const static map<int, string> status_messages = {
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

void usage( const char * argv0 )
{
  cerr << "Usage: " << argv0 << endl
       << "       " << "[-s|--no-status] [-d|--no-download] [-S|--sandboxed]" << endl
       << "       " << "[[-j|--jobs=<N>] [-e|--engine=<name>[=ENGINE_ARGS]]]... " << endl
       << "       " << "[[-j|--jobs=<N>] [-f|--fallback-engine=<name>[=ENGINE_ARGS]]]..." << endl
       << "       " << "[-T|--timeout=<t>] [-m|--timeout-multiplier=<N>] IP PORT THUNKS..." << endl
       << endl
       << "Available engines:" << endl
       << "  - local   Executes the jobs on the local machine" << endl
       << "  - lambda  Executes the jobs on AWS Lambda" << endl
       << "  - remote  Executes the jobs on a remote machine" << endl
       << "  - meow    Executes the jobs on AWS Lambda with long-running workers" << endl
       << "  - gcloud  Executes the jobs on Google Cloud Functions" << endl
       << "  - baseline Baseline for SimpleDB" << endl
       << endl
       << "Environment variables:" << endl
       << "  - " << FORCE_NO_STATUS << endl
       << "  - " << FORCE_DEFAULT_ENGINE << endl
       << "  - " << FORCE_TIMEOUT << endl
       << endl;
}

void check_rlimit_nofile( const size_t max_jobs )
{
  struct rlimit limits;
  CheckSystemCall( "getrlimit", getrlimit( RLIMIT_NOFILE, &limits ) );

  size_t target_nofile = max( max_jobs * 3, limits.rlim_cur );
  target_nofile = min( target_nofile, limits.rlim_max );

  if ( limits.rlim_cur < target_nofile ) {
    /* cerr << "Increasing the maximum number of allowed file descriptors from "
         << limits.rlim_cur << " to " << target_nofile << ".\n"; */
    limits.rlim_cur = target_nofile;
    CheckSystemCall( "setrlimit", setrlimit( RLIMIT_NOFILE, &limits ) );
  }
}

using EngineInfo = tuple<string, string, size_t>;

EngineInfo parse_engine( const string & name, const size_t max_jobs )
{
  string::size_type eqpos = name.find( '=' );
  if ( eqpos == string::npos ) {
    return make_tuple( move( name ), move( string {} ), max_jobs );
  }
  else {
    return make_tuple( name.substr( 0, eqpos ), name.substr( eqpos + 1 ), max_jobs );
  }
}

unique_ptr<ExecutionEngine> make_execution_engine( const EngineInfo & engine )
{
  const string & engine_name = get<0>( engine );
  const string & engine_params = get<1>( engine );
  const size_t max_jobs = get<2>( engine );

  if ( engine_name == "local" ) {
    const bool mixed = (engine_params == "mixed");
    return make_unique<LocalExecutionEngine>( mixed, max_jobs );
  }
  else if ( engine_name == "lambda" ) {
    return make_unique<AWSLambdaExecutionEngine>( max_jobs, AWSCredentials(),
      engine_params.length() ? engine_params : AWS::region() );
  }
  else if ( engine_name == "baseline" ) {
    if ( engine_params.length() == 0 ) {
      throw runtime_error( "remote: missing host ip" );
    }

    vector<Address> workers;
    for (auto &addr : split(engine_params, "&"))
    {
      uint16_t port = 8080;
      string::size_type colonpos = addr.find( ':' );
      string host_ip = addr.substr( 0, colonpos );

      if ( colonpos != string::npos ) {
        port = stoi( addr.substr( colonpos + 1 ) );
      }

      workers.emplace_back(host_ip, port);
    }
    if (workers.size() != max_jobs)
      throw runtime_error("baseline: incorrect args");

    return make_unique<BaselineExecutionEngine>( max_jobs, workers );
  }
  else if ( engine_name == "sdb" ) {
    if ( engine_params.length() == 0 ) {
      throw runtime_error( "remote: missing host ip" );
    }

    vector<Address> workers;
    for (auto &addr : split(engine_params, "&"))
    {
      uint16_t port = 8080;
      string::size_type colonpos = addr.find( ':' );
      string host_ip = addr.substr( 0, colonpos );

      if ( colonpos != string::npos ) {
        port = stoi( addr.substr( colonpos + 1 ) );
      }

      workers.emplace_back(host_ip, port);
    }

    return make_unique<SimpleDBExecutionEngine>( max_jobs, workers );
  }
  else if ( engine_name == "remote" ) {
    if ( engine_params.length() == 0 ) {
      throw runtime_error( "remote: missing host ip" );
    }

    uint16_t port = 9924;
    string::size_type colonpos = engine_params.find( ':' );
    string host_ip = engine_params.substr( 0, colonpos );

    if ( colonpos != string::npos ) {
      port = stoi( engine_params.substr( colonpos + 1 ) );
    }

    return make_unique<GGExecutionEngine>( max_jobs, Address { host_ip, port } );
  }
  else if ( engine_name == "meow" ) {
    if ( engine_params.length() == 0 ) {
      throw runtime_error( "meow: missing host public ip" );
    }

    uint16_t port = 9925;
    string::size_type colonpos = engine_params.find( ':' );
    string host_ip = engine_params.substr( 0, colonpos );

    if ( colonpos != string::npos ) {
      port = stoi( engine_params.substr( colonpos + 1 ) );
    }

    return make_unique<MeowExecutionEngine>( max_jobs, AWSCredentials(),
      AWS::region(), Address { host_ip, port } );
  }
  else if ( engine_name == "gcloud" ) {
    return make_unique<GCFExecutionEngine>( max_jobs,
      safe_getenv("GG_GCLOUD_FUNCTION") );
  }
  else {
    throw runtime_error( "unknown execution engine" );
  }
}

void sigint_handler( int )
{
  throw runtime_error( "killed by signal" );
}

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

    signal( SIGINT, sigint_handler );

    int timeout = ( getenv( FORCE_TIMEOUT ) != nullptr )
                  ? stoi( safe_getenv( FORCE_TIMEOUT ) ) : 0;
    size_t timeout_multiplier = 1;
    bool status_bar = !( getenv( FORCE_NO_STATUS ) != nullptr );

    size_t total_max_jobs = 0;
    size_t max_jobs = thread::hardware_concurrency();
    vector<EngineInfo> engines_info;
    vector<EngineInfo> fallback_engines_info;

    struct option long_options[] = {
      { "no-status",          no_argument,       nullptr, 's' },
      { "sandboxed",          no_argument,       nullptr, 'S' },
      { "jobs",               required_argument, nullptr, 'j' },
      { "timeout",            required_argument, nullptr, 'T' },
      { "timeout-multiplier", required_argument, nullptr, 'T' },
      { "engine",             required_argument, nullptr, 'e' },
      { "fallback-engine",    required_argument, nullptr, 'f' },
      { nullptr,              0,                 nullptr,  0  },
    };

    while ( true ) {
      const int opt = getopt_long( argc, argv, "sSj:T:e:d", long_options, NULL );

      if ( opt == -1 ) {
        break;
      }

      switch ( opt ) {
      case 's':
        status_bar = false;
        break;

      case 'S':
        setenv( "GG_SANDBOXED", "1", true );
        break;

      case 'j':
        max_jobs = stoul( optarg );
        break;

      case 'T':
        timeout = stoi( optarg );
        break;

      case 'e':
        engines_info.emplace_back( move( parse_engine( optarg, max_jobs ) ) );
        total_max_jobs += max_jobs;
        break;

      case 'f':
        fallback_engines_info.emplace_back( move( parse_engine( optarg, max_jobs ) ) );
        total_max_jobs += max_jobs;
        break;

      case 'm':
        timeout_multiplier = stoul( optarg );
        break;

      default:
        throw runtime_error( "invalid option" );
      }
    }

    check_rlimit_nofile( total_max_jobs );

    gg::models::init();

    vector<unique_ptr<ExecutionEngine>> execution_engines;
    vector<unique_ptr<ExecutionEngine>> fallback_engines;
    unique_ptr<StorageBackend> storage_backend;
    bool remote_execution = false;

    if ( engines_info.size() == 0 ) {
      if ( getenv( FORCE_DEFAULT_ENGINE ) != nullptr ) {
        engines_info.emplace_back( move( parse_engine(
          safe_getenv( FORCE_DEFAULT_ENGINE ), max_jobs ) ) );
      }
      else {
        engines_info.emplace_back( make_tuple( "local", "", max_jobs ) );
      }
    }

    for ( const auto & engine : engines_info ) {
      execution_engines.emplace_back( move( make_execution_engine( engine ) ) );
      remote_execution |= execution_engines.back()->is_remote();
    }

    for ( const auto & engine : fallback_engines_info ) {
      fallback_engines.emplace_back( move( make_execution_engine( engine ) ) );
      remote_execution |= fallback_engines.back()->is_remote();
    }

    if ( remote_execution ) {
      storage_backend = StorageBackend::create_backend( gg::remote::storage_backend_uri() );
    }

    if (argc <= optind + 1) {
      usage( argv[ 0 ] );
      return EXIT_FAILURE;
    }

    int port_argv = stoi( argv[ optind + 1 ] );
    if ( port_argv <= 0 or port_argv > numeric_limits<uint16_t>::max() ) {
      throw runtime_error( "invalid port" );
    }
    Address listen_addr { argv[ optind ], static_cast<uint16_t>( port_argv ) };
    ExecutionLoop exec_loop;

    Scheduler scheduler(exec_loop,
      move(execution_engines),
      move(fallback_engines),
      move(storage_backend),
      std::chrono::milliseconds { timeout * 1000 },
      timeout_multiplier, status_bar
    );

    exec_loop.make_listener( listen_addr,
      [&scheduler] ( ExecutionLoop & loop, TCPSocket && socket ) -> bool {
        auto request_parser = make_shared<HTTPRequestParser>();

        auto connection = loop.add_connection<TCPSocket>( move( socket ),
          [&scheduler, request_parser] ( shared_ptr<TCPConnection> connection, string && data ) {
            request_parser->parse( data );

            while ( not request_parser->empty() ) {
              HTTPRequest request { move( request_parser->front() ) };
              request_parser->pop();

              const string & first_line = request.first_line();
              const string::size_type first_space = first_line.find( ' ' );
              const string::size_type last_space = first_line.rfind( ' ' );

              if ( first_space == string::npos or last_space == string::npos or first_line.substr( 0, first_space ) != "POST") {
                /* wrong http request */
                connection->enqueue_write( get_canned_response( 400, request ) );
                continue;
              }

              const string & thunk_hash = first_line.substr( first_space + 2,
                                                                  last_space - first_space - 2 );
              roost::path thunk_path = gg::paths::blob(thunk_hash);
              if (!ThunkReader::is_thunk(thunk_path)) {
                /* wrong http request */
                connection->enqueue_write( get_canned_response( 400, request ) );
                continue;
              }

              auto tracker = make_shared<Tracker>(thunk_hash, connection);
              tracker->set_request(request);
              scheduler.add_dag(tracker);
            }

            return true;
          },
          [] () {
            /* error callback */
            cerr << "error" << endl;
          },
          [] () {
            /* close callback */
            cerr << "closed" << endl;
          } );

          return true;
      } );

    while (true) {
      auto finished_dags = scheduler.run_once();
      for (auto & dag : finished_dags) {
        auto connection = dag->get_connection();

        HTTPResponse response;
        const string final_hash = dag->reduce();
        response.set_request( dag->get_request() );
        response.set_first_line( "HTTP/1.1 200 OK" );
        response.add_header( HTTPHeader{ "Content-Length", to_string(final_hash.length())} );
        response.add_header( HTTPHeader{ "Content-Type", "text/plain" } );
        response.done_with_headers();
        response.read_in_body( final_hash );
        assert( response.state() == COMPLETE );

        connection->enqueue_write(response.str());
        dag->print_status();
      }
    }

    return EXIT_SUCCESS;
  }
  catch ( const exception &  e ) {
    print_exception( argv[ 0 ], e );
    return EXIT_FAILURE;
  }
}