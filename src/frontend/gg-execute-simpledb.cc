/* -*-mode:c++; tab-width: 2; indent-tabs-mode: nil; c-basic-offset: 2 -*- */

#include <iostream>
#include <string>
#include <sstream>
#include <memory>
#include <sys/fcntl.h>
#include <getopt.h>
#include <vector>
#include <unordered_set>

#include "execution/response.hh"
#include "net/requests.hh"
#include "storage/backend.hh"
#include "thunk/ggutils.hh"
#include "thunk/factory.hh"
#include "thunk/thunk_reader.hh"
#include "thunk/thunk_writer.hh"
#include "thunk/thunk.hh"
#include "util/child_process.hh"
#include "util/digest.hh"
#include "util/exception.hh"
#include "util/path.hh"
#include "util/temp_dir.hh"
#include "util/temp_file.hh"
#include "util/timelog.hh"
#include "util/util.hh"

#include "cpplambda.h"

using namespace std;
using namespace gg;
using namespace gg::thunk;
using namespace simpledb::proto;
using ReductionResult = gg::cache::ReductionResult;

const bool sandboxed = ( getenv( "GG_SANDBOXED" ) != NULL );
const string temp_dir_template = "/tmp/thunk-execute";
const string temp_file_template = "/tmp/thunk-file";

stringstream _stdoutstream;

vector<string> execute_thunk( const Thunk & original_thunk )
{
  Thunk thunk = original_thunk;

  // if ( not thunk.can_be_executed() ) {
  //   /* Let's see if we can redudce this thunk to an order one thunk by updating
  //   the infiles */
  //   for ( const Thunk::DataItem & dep_item : thunk.thunks() ) {
  //     /* let's check if we have a reduction of this infile */
  //     vector<ThunkOutput> new_outputs;
  //
  //     auto result = gg::cache::check( dep_item.first );
  //
  //     if ( not result.initialized() or
  //          gg::hash::type( result->hash ) == gg::ObjectType::Thunk ) {
  //       throw runtime_error( "thunk is not executable and cannot be "
  //                            "reduced to an executable thunk" );
  //     }
  //
  //     thunk.update_data( dep_item.first, result->hash );
  //   }
  //
  //   thunk.set_hash( ThunkWriter::write( thunk ) );
  //
  //   cerr << "thunk:" << original_thunk.hash() << " reduced to "
  //        << "thunk:" << thunk.hash() << "." << endl;
  // }

  /* when executing the thunk, we create a temp directory, and execute the thunk
     in that directory. then we take the outfile, compute the hash, and move it
     to the .gg directory. */

  // PREPARING THE ENV
  TempDirectory exec_dir { temp_dir_template };
  roost::path exec_dir_path { exec_dir.name() };
  roost::path outfile_path { "output" };

  // EXECUTING THE THUNK
  if ( not sandboxed ) {
    ChildProcess process {
      thunk.hash(),
      [thunk, &exec_dir_path]() {
        roost::create_directories( exec_dir_path );
        CheckSystemCall( "chdir", chdir( exec_dir_path.string().c_str() ) );
        return thunk.execute();
      }
    };

    while ( not process.terminated() ) {
      process.wait();
    }

    if ( process.exit_status() ) {
      try {
        process.throw_exception();
      }
      catch ( const exception & ex ) {
        throw_with_nested( ExecutionError {} );
      }
    }
  }
  else {
    auto allowed_files = thunk.get_allowed_files();

    SandboxedProcess process {
      "execute(" + thunk.hash().substr( 0, 5 ) + ")",
      allowed_files,
      [thunk]() {
        return thunk.execute();
      },
      [&exec_dir_path] () {
        roost::create_directories( exec_dir_path );
        CheckSystemCall( "chdir", chdir( exec_dir_path.string().c_str() ) );
      }
    };

    try {
      process.execute();
    }
    catch( const exception & ex ) {
      throw_with_nested( ExecutionError {} );
    }
  }

  vector<string> output_hashes;

  // GRABBING THE OUTPUTS & CREATING CACHE ENTRIES
  for ( const string & output : thunk.outputs() ) {
    roost::path outfile { exec_dir_path / output };

    if ( not roost::exists( outfile ) ) {
      throw ExecutionError {};
    }

    /* let's check if the output is a thunk or not */
    string outfile_hash = gg::hash::file( outfile );
    roost::path outfile_gg = gg::paths::blob( outfile_hash );

    if ( not roost::exists( outfile_gg ) ) {
      roost::move_file( outfile, outfile_gg );
    } else {
      roost::remove( outfile );
    }

    output_hashes.emplace_back( move( outfile_hash ) );
  }

  for ( size_t i = thunk.outputs().size() - 1; i < thunk.outputs().size(); i-- ) {
    const string & output = thunk.outputs().at( i );
    const string & outfile_hash = output_hashes.at( i );

    gg::cache::insert( original_thunk.output_hash( output ), outfile_hash );
    if ( original_thunk.hash() != thunk.hash() ) {
      gg::cache::insert( thunk.output_hash( output ), outfile_hash );
    }

    if ( i == 0 ) {
      gg::cache::insert( original_thunk.hash(), outfile_hash );
      if ( original_thunk.hash() != thunk.hash() ) {
        gg::cache::insert( thunk.hash(), outfile_hash );
      }
    }
  }

  return output_hashes;
}

void do_cleanup( const Thunk & thunk )
{
  unordered_set<string> infile_hashes;

  infile_hashes.emplace( thunk.hash() );

  for ( const Thunk::DataItem & item : thunk.values() ) {
    infile_hashes.emplace( item.first );
  }

  for ( const Thunk::DataItem & item : thunk.executables() ) {
    infile_hashes.emplace( item.first );
  }

  for ( const string & blob : roost::list_directory( gg::paths::blobs() ) ) {
    const roost::path path = gg::paths::blob( blob );
    if ( ( not roost::is_directory( path ) ) and infile_hashes.count( blob ) == 0 ) {
      roost::remove( path );
    }
  }
}

void upload_output(ExecResponse& _response, const vector<string> & output_hashes)
{
  auto *f_output = _response.mutable_f_output();
  for (const string& output_hash : output_hashes)
  {
    auto request = f_output->Add();
    request->set_key(output_hash);
    request->set_val(gg::paths::blob(output_hash).string());
    request->set_immutable(false); // TODO: figure this out!
    request->set_executable(true); // TODO: figure this out!
  }
}

void upload_output(ExecResponse& _response, const unordered_map<string, string> & output_values)
{
  auto *kw_output = _response.mutable_f_output();
  for (auto &output_kv : output_values)
  {
    auto request = kw_output->Add();
    request->set_key(output_kv.first);
    request->set_val(output_kv.second);
    request->set_immutable(false); // TODO: figure this out!
    request->set_executable(true); // TODO: figure this out!
  }
}

void usage( const char * argv0 )
{
  cerr << "Usage: " << argv0 << " [options] THUNK-HASH..." << endl
  << endl
  << "Options: " << endl
  << " -C, --cleanup           Remove unnecessary blobs in .gg dir" << endl
  << " -T, --timelog           Produce timing log for this execution" << endl
  << " -u, --upload-timelog    Upload timing log for this exection" << endl
  << endl;
}

int gg_execute_main( int argc, char * argv[], ExecResponse& _response)
{
  int ret;
  gg::protobuf::ExecutionResponse execution_response;

  try {
    if ( argc <= 0 ) {
      abort();
    }

    if ( argc < 2 ) {
      usage( argv[ 0 ] );
      return to_underlying( JobStatus::OperationalFailure );
    }

    bool cleanup = false;
    bool upload_timelog = false;
    Optional<TimeLog> timelog;

    const option command_line_options[] = {
      { "cleanup",          no_argument, nullptr, 'C' },
      { "timelog",          no_argument, nullptr, 'T' },
      { nullptr, 0, nullptr, 0 },
    };

    while ( true ) {
      const int opt = getopt_long( argc, argv, "uCT", command_line_options, nullptr );

      if ( opt == -1 ) {
        break;
      }

      switch ( opt ) {
      case 'C': cleanup = true; break;
      case 'T': timelog.reset(); break;
      case 'u': upload_timelog = true; break;

      default:
        throw runtime_error( "invalid option: " + string { argv[ optind - 1 ] } );
      }
    }

    vector<string> thunk_hashes;

    for ( int i = optind; i < argc; i++ ) {
      thunk_hashes.push_back( argv[ i ] );
    }

    if ( thunk_hashes.size() == 0 ) {
      usage( argv[ 0 ] );
      return to_underlying( JobStatus::OperationalFailure );
    }

    gg::models::init();

    for ( const string & thunk_hash : thunk_hashes ) {
      /* take out an advisory lock on the thunk, in case
         other gg-execute processes are running at the same time */
      const string thunk_path = gg::paths::blob( thunk_hash ).string();
      FileDescriptor raw_thunk { CheckSystemCall( "open( " + thunk_path + " )",
                                                  open( thunk_path.c_str(), O_RDONLY ) ) };
      raw_thunk.block_for_exclusive_lock();

      Thunk thunk = ThunkReader::read( thunk_path );

      if ( timelog.initialized() ) { timelog->add_point( "read_thunk" ); }

      if ( cleanup ) {
        do_cleanup( thunk );
      }

      if ( timelog.initialized() ) { timelog->add_point( "do_cleanup" ); }

      if ( timelog.initialized() ) { timelog->add_point( "get_dependencies" ); }

      vector<string> output_hashes = execute_thunk( thunk );

      if ( timelog.initialized() ) { timelog->add_point( "execute" ); }

      upload_output(_response, output_hashes);

      if ( timelog.initialized() ) { timelog->add_point( "upload_output" ); }

      if ( timelog.initialized() and upload_timelog) {
        unordered_map<string, string> output_values;
        output_values["timelog-" + thunk_hash] = timelog->str();
        upload_output(_response, output_values);
      }
      else if ( timelog.initialized() ) {
        _stdoutstream << timelog->str() << endl;
      }

      auto thunk_response = execution_response.add_executed_thunks();
      thunk_response->set_thunk_hash(thunk_hash);
      for (const auto& output_tag: thunk.outputs())
      {
        Optional<cache::ReductionResult> result = gg::cache::check(gg::hash::for_output(thunk_hash, output_tag));

        if ( not result.initialized() ) {
          throw runtime_error( "output not found" );
        }

        auto output_item = thunk_response->add_outputs();
        output_item->set_tag(output_tag);
        output_item->set_hash(result->hash);
      }
    }

    ret = to_underlying( JobStatus::Success );
  }
  catch ( const FetchDependenciesError & e ) {
    print_nested_exception( e );
    ret = to_underlying( JobStatus::FetchDependenciesFailure );
  }
  catch ( const ExecutionError & e ) {
    print_nested_exception( e );
    ret = to_underlying( JobStatus::ExecutionFailure );
  }
  catch ( const UploadOutputError & e ) {
    print_nested_exception( e );
    ret = to_underlying( JobStatus::UploadOutputFailure );
  }
  catch ( const exception & e ) {
    print_exception( argv[ 0 ], e );
    ret = to_underlying( JobStatus::OperationalFailure );
  }

  execution_response.set_return_code(ret);
  execution_response.set_stdout(_stdoutstream.str());

  _response.set_return_code(0);
  _response.set_return_output(execution_response.SerializeAsString());

  return 0;
}

#define MAX_CMD_ARGS 256
void lambda_exec(const ExecArgs& params,
                ExecResponse& resp)
{
  int i = 0;
  char* argv[MAX_CMD_ARGS];
  vector<string> vargs;

  vargs.push_back("gg-execute-simpledb.cc");
  argv[i] = &(vargs[i][0]);
  i++;

  for (const auto &arg : params.args())
  {
    vargs.push_back(arg);
    argv[i] = &(vargs[i][0]);
    i++;

    if (i >= MAX_CMD_ARGS)
      throw runtime_error("Too many arguments");
  }

  for (const string& arg : params.fargs())
  {
    vargs.push_back(arg);
    argv[i] = &(vargs[i][0]);
    i++;
  
    if (i >= MAX_CMD_ARGS)
      throw runtime_error("Too many arguments");
  }

  auto ret = gg_execute_main(i, argv, resp);
  resp.set_return_code(ret);
  resp.set_return_output(move(_stdoutstream.str()));
}
