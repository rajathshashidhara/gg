/* -*-mode:c++; tab-width: 2; indent-tabs-mode: nil; c-basic-offset: 2 -*- */

#include <iostream>
#include <vector>
#include <chrono>
#include <thread>
#include <tuple>
#include <cstdlib>
#include <getopt.h>
#include <sys/time.h>
#include <sys/resource.h>

#include "execution/reductor.hh"
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

void usage( char * argv0 )
{
  cerr << argv0 << " IP STORAGE-PORT SCHEDULER-PORT THUNK" << endl;
}

int main( int argc, char * argv[] )
{
  try {
    if ( argc <= 0 ) {
      abort();
    }

    if ( argc < 5 ) {
      usage( argv[ 0 ] );
      return EXIT_FAILURE;
    }

    int storage_port = stoi( argv[ 2 ] );
    int sched_port = stoi( argv[ 3 ] );
    if ( storage_port <= 0 or storage_port > numeric_limits<uint16_t>::max() ) {
      throw runtime_error( "invalid port" );
    }
    if ( sched_port <= 0 or sched_port > numeric_limits<uint16_t>::max() ) {
      throw runtime_error( "invalid port" );
    }
    Address storage_addr { argv[ 1 ], static_cast<uint16_t>( storage_port ) };
    Address sched_addr { argv[ 1 ], static_cast<uint16_t>( sched_port ) };

    vector<string> target_filenames;
    vector<string> target_hashes;
    for (int i = 4; i < argc; i++)
    {
      target_filenames.emplace_back(argv[i]);
    }
    for ( const string & target_filename : target_filenames ) {
      string thunk_hash;

      /* first check if this file is actually a placeholder */
      Optional<ThunkPlaceholder> placeholder = ThunkPlaceholder::read( target_filename );

      if ( not placeholder.initialized() ) {
        if( not ThunkReader::is_thunk( target_filename ) ) {
          cerr << "not a thunk: " << target_filename << endl;
          continue;
        }
        else {
          thunk_hash = gg::hash::compute( target_filename, gg::ObjectType::Thunk );
        }
      }
      else {
        thunk_hash = placeholder->content_hash();
      }

      target_hashes.emplace_back( move( thunk_hash ) );
    }

    ExecutionGraph dep_graph;
    cerr << "\u2192 Loading the thunks... ";
    auto graph_load_time = time_it<milliseconds>(
      [&dep_graph, &target_hashes] ()
      {
        for ( const string & hash : target_hashes ) {
          dep_graph.add_thunk( hash );
        }
      } ).count();
    cerr << " done (" << graph_load_time << " ms)." << endl;

    unique_ptr<StorageBackend> storage_backend = StorageBackend::create_backend( gg::remote::storage_backend_uri() );
    unique_ptr<StorageBackend> sched_storage_backend = StorageBackend::create_backend("gg://username:password@" + storage_addr.str());

    vector<storage::PutRequest> upload_requests;
    size_t total_size = 0;
    for ( const string & dep : dep_graph.value_dependencies() ) {
      if ( storage_backend->is_available( dep ) ) {
        continue;
      }

      total_size += gg::hash::size( dep );
      upload_requests.push_back( { gg::paths::blob( dep ), dep,
                                  gg::hash::to_hex( dep ) } );
    }
    for ( const string & dep : dep_graph.executable_dependencies() ) {
      if ( storage_backend->is_available( dep ) ) {
        continue;
      }

      total_size += gg::hash::size( dep );
      upload_requests.push_back( { gg::paths::blob( dep ), dep,
                                  gg::hash::to_hex( dep ) } );
    }

    if ( upload_requests.size() > 0 ) {
      const string plural = upload_requests.size() == 1 ? "" : "s";
      cerr << "\u2197 Uploading " << upload_requests.size() << " file" << plural
          << " (" << format_bytes( total_size ) << ")... ";
      auto upload_time = time_it<milliseconds>(
        [&upload_requests, &storage_backend]()
        {
          storage_backend->put(
            upload_requests,
            [&storage_backend] ( const storage::PutRequest & upload_request )
            { storage_backend->set_available( upload_request.object_key ); }
          );
        }
      );
      cerr << "done (" << upload_time.count() << " ms)." << endl;
    }

    upload_requests.clear();
    for ( const string & dep : dep_graph.get_thunk_hashes() ) {
      if ( sched_storage_backend->is_available( dep ) ) {
        continue;
      }

      total_size += gg::hash::size( dep );
      upload_requests.push_back( { gg::paths::blob( dep ), dep,
                                  gg::hash::to_hex( dep ) } );
    }
    if ( upload_requests.size() > 0 ) {
      cerr << "No files to upload." << endl;
      const string plural = upload_requests.size() == 1 ? "" : "s";
      cerr << "\u2197 Uploading " << upload_requests.size() << " file" << plural
          << " (" << format_bytes( total_size ) << ")... ";
      auto upload_time = time_it<milliseconds>(
        [&upload_requests, &sched_storage_backend]()
        {
          sched_storage_backend->put(
            upload_requests,
            [&sched_storage_backend] ( const storage::PutRequest & upload_request )
            { sched_storage_backend->set_available( upload_request.object_key ); }
          );
        }
      );
      cerr << "done (" << upload_time.count() << " ms)." << endl;
    }

    int targets_left = target_hashes.size();
    unordered_map<string, string> reduction_result;

    ExecutionLoop loop;
    for (auto & thunk_hash : target_hashes) {
      HTTPRequest request;
      request.set_first_line( "POST /" + thunk_hash + " HTTP/1.1");
      request.add_header( HTTPHeader{ "Content-Length", to_string( 0 ) } );
      request.add_header( HTTPHeader{ "Host", "gg-run-server" } );
      request.done_with_headers();
      request.read_in_body( "" );
      assert( request.state() == COMPLETE );

      loop.make_http_request<TCPConnection>( thunk_hash,
        sched_addr, request,
        [&storage_backend, &targets_left, &reduction_result]( const uint64_t, const string & thunk_hash,
             const HTTPResponse & http_response ) -> bool {

          if (http_response.status_code() != "200") {
            cerr << "Failed to execute: " << thunk_hash << " " << http_response.status_code() <<  endl;
            abort();
          }

          const string final_hash = http_response.body();
          reduction_result[thunk_hash] = final_hash;
          targets_left--;

          return true;
        },
        [] ( const uint64_t, const string & thunk_hash )
        {
          cerr << "Failed to execute: " << thunk_hash << endl;
        });
    }

    cerr << "\u2192 Executing... ";
    auto execution_time = time_it<milliseconds>(
      [&loop, &targets_left]()
      {
        while (true) {
          if (!targets_left)
            break;

          loop.loop_once(-1);
        }
      }
    );
    cerr << "done (" << execution_time.count() << " ms)." << endl;

    vector<storage::GetRequest> download_requests;
    total_size = 0;

    for (auto & it : reduction_result ) {
      const string& hash = it.second;
      if ( not roost::exists( gg::paths::blob( hash ) ) ) {
        download_requests.push_back( { hash, gg::paths::blob( hash ) } );
        total_size += gg::hash::size( hash );
      }
    }

    if ( download_requests.size() > 0 ) {
      const string plural = download_requests.size() == 1 ? "" : "s";
      cerr << "\u2198 Downloading output file" << plural
          << " (" << format_bytes( total_size ) << ")... ";
      auto download_time = time_it<milliseconds>(
        [&download_requests, &storage_backend]()
        {
          storage_backend->get( download_requests );
        }
      );
      cerr << "done (" << download_time.count() << " ms)." << endl;
    }

    for (size_t i = 0; i < target_hashes.size(); i++) {
      roost::copy_then_rename( gg::paths::blob(reduction_result[target_hashes[i]]), target_filenames[i]);

      /* HACK this is a just a dirty hack... it's not always right */
      roost::make_executable( target_filenames[ i ] );
    }

    return EXIT_SUCCESS;
  }
  catch (const exception &  e ) {
    print_exception( argv[ 0 ], e );
    return EXIT_FAILURE;
  }
}


