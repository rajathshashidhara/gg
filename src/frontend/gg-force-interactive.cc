/* -*-mode:c++; tab-width: 2; indent-tabs-mode: nil; c-basic-offset: 2 -*- */

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

using namespace std;
using namespace std::chrono;
using namespace gg::thunk;

constexpr char FORCE_NO_STATUS[] = "GG_FORCE_NO_STATUS";
constexpr char FORCE_DEFAULT_ENGINE[] = "GG_FORCE_DEFAULT_ENGINE";
constexpr char FORCE_MAX_JOBS[] = "GG_FORCE_MAX_JOBS";
constexpr char FORCE_TIMEOUT[] = "GG_FORCE_TIMEOUT";

queue<string> target_hashes;
mutex         hash_vec_lock;

static void push_target_hash(string hash)
{
  lock_guard<mutex> lk(hash_vec_lock);
  target_hashes.push(hash);
}

static Optional<string> pop_target_hash()
{
  Optional<string> hash;

  lock_guard<mutex> lk(hash_vec_lock);
  if (!target_hashes.empty()) {
    hash.initialize(target_hashes.front());
    target_hashes.pop();
  }

  return hash;
}

void sigint_handler( int )
{
  throw runtime_error( "killed by signal" );
}

void usage( const char * argv0 )
{
  cerr << "Usage: " << argv0 << endl
       << "       " << "[-s|--no-status] [-d|--no-download] [-S|--sandboxed]" << endl
       << "       " << "[[-j|--jobs=<N>] [-e|--engine=<name>[=ENGINE_ARGS]]]... " << endl
       << "       " << "[[-j|--jobs=<N>] [-f|--fallback-engine=<name>[=ENGINE_ARGS]]]..." << endl
       << "       " << "[-T|--timeout=<t>] [-m|--timeout-multiplier=<N>] THUNKS..." << endl
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

void cli()
{
  for (;;) {
    cout << "$>";

    string inp;
    getline(cin, inp);

    push_target_hash(inp);
  }
}

int main( int argc, char * argv[] )
{
  try {
    if ( argc <= 0 ) {
      abort();
    }

    if ( argc < 1 ) {
      usage( argv[ 0 ] );
      return EXIT_FAILURE;
    }

    signal( SIGINT, sigint_handler );

    int timeout = ( getenv( FORCE_TIMEOUT ) != nullptr )
                  ? stoi( safe_getenv( FORCE_TIMEOUT ) ) : 0;
    size_t timeout_multiplier = 1;
    bool status_bar = !( getenv( FORCE_NO_STATUS ) != nullptr );
    // bool no_download = false;

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
      // { "no-download",        no_argument,       nullptr, 'd' },
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

      // case 'd':
      //   no_download = true;
      //   break;

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

    ExecutionLoop loop;
    Scheduler scheduler(loop,
      move(execution_engines),
      move(fallback_engines),
      move(storage_backend),
      std::chrono::milliseconds { timeout * 1000 },
      timeout_multiplier, status_bar
    );

    thread cli_thread(cli);

    for (;;)
    {
      Optional<string> new_hash;

      if (!target_hashes.empty()) {
        new_hash = pop_target_hash();
      }

      if (new_hash.initialized()) {
        vector<string> hashes;
        hashes.push_back(new_hash.get());

        scheduler.add_dag(hashes);
      }

      auto finished_dags = scheduler.run_once();
      for (auto & dag : finished_dags) {
        dag->print_status();
      }
    }

    cli_thread.join();

    return EXIT_SUCCESS;
  }
  catch ( const exception &  e ) {
    print_exception( argv[ 0 ], e );
    return EXIT_FAILURE;
  }
}