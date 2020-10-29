#include <thread>
#include <vector>
#include <fcntl.h>
#include <sys/types.h>

#include "gg_objectstore.hh"
#include "socket.hh"
#include "http_request.hh"
#include "http_response_parser.hh"
#include <util/exception.hh>

using namespace std;

void GGObjectStoreClient::upload_files(
    const std::vector<storage::PutRequest>& upload_requests,
    const std::function<void(const storage::PutRequest&)>& success_callback)
{
  const size_t thread_count = config_.max_threads;
  const size_t batch_size = config_.max_batch_size;

  vector<thread> threads;
  for ( size_t thread_index = 0; thread_index < thread_count; thread_index++ ) {
    if ( thread_index < upload_requests.size() ) {
      threads.emplace_back(
        [&] (const size_t index) {
          for ( size_t first_file_idx = index;
                first_file_idx < upload_requests.size();
                first_file_idx += thread_count * batch_size ) {

            HTTPResponseParser responses;
            TCPSocket conn;
            conn.connect(config_.address_);

            for ( size_t file_id = first_file_idx;
                  file_id < min( upload_requests.size(), first_file_idx + thread_count * batch_size );
                  file_id += thread_count ) {
              const string & filename = upload_requests.at( file_id ).filename.string();
              const string & object_key = upload_requests.at( file_id ).object_key;

              string contents;
              FileDescriptor file { CheckSystemCall( "open " + filename, open( filename.c_str(), O_RDONLY ) ) };
              while ( not file.eof() ) { contents.append( file.read() ); }
              file.close();

              HTTPRequest request;
              request.set_first_line( "POST /" + object_key + " HTTP/1.1" );
              request.add_header( HTTPHeader{ "Content-Length", to_string( contents.size() ) } );
              request.add_header( HTTPHeader{ "Content-Type", "application/octet-stream" } );
              request.add_header( HTTPHeader{ "Host", config_.address_.str() } );
              request.done_with_headers();

              request.read_in_body( contents );
              assert( request.state() == COMPLETE );

              responses.new_request_arrived( request );

              conn.write( request.str() );
            }

            size_t response_count = 0;

            while ( responses.pending_requests() ) {
              /* drain responses */
              responses.parse( conn.read() );
              if ( not responses.empty() ) {
                if ( responses.front().first_line() != "HTTP/1.1 200 OK" ) {
                  throw runtime_error( "HTTP failure in GGObjectStoreClient::upload_files(): " + responses.front().first_line() );
                }
                else {
                  const size_t response_index = first_file_idx + response_count * thread_count;
                  success_callback( upload_requests[ response_index ] );
                }

                responses.pop();
                response_count++;
              }
            }
          }
        }, thread_index
      );
    }
  }

  for ( auto & thread : threads ) {
    thread.join();
  }
}

void GGObjectStoreClient::download_files(
    const std::vector<storage::GetRequest>& download_requests,
    const std::function<void(const storage::GetRequest&)>& success_callback)
{
  const size_t thread_count = config_.max_threads;
  const size_t batch_size = config_.max_batch_size;

  vector<thread> threads;
  for ( size_t thread_index = 0; thread_index < thread_count; thread_index++ ) {
    if ( thread_index < download_requests.size() ) {
      threads.emplace_back(
        [&] (const size_t index) {
          for ( size_t first_file_idx = index;
                first_file_idx < download_requests.size();
                first_file_idx += thread_count * batch_size ) {

            HTTPResponseParser responses;
            TCPSocket conn;
            conn.connect(config_.address_);

            for ( size_t file_id = first_file_idx;
                  file_id < min( download_requests.size(), first_file_idx + thread_count * batch_size );
                  file_id += thread_count ) {
              const string & object_key = download_requests.at( file_id ).object_key;

              HTTPRequest request;
              request.set_first_line( "GET /" + object_key + " HTTP/1.1" );
              request.add_header( HTTPHeader{ "Content-Length", to_string( 0 ) } );
              request.add_header( HTTPHeader{ "Content-Type", "application/octet-stream" } );
              request.add_header( HTTPHeader{ "Host", config_.address_.str() } );
              request.done_with_headers();

              request.read_in_body( "" );
              assert( request.state() == COMPLETE );

              responses.new_request_arrived( request );

              conn.write( request.str() );
            }

            size_t response_count = 0;

            while ( responses.pending_requests() ) {
              /* drain responses */
              responses.parse( conn.read() );
              if ( not responses.empty() ) {
                if ( responses.front().first_line() != "HTTP/1.1 200 OK" ) {
                  throw runtime_error( "HTTP failure in GGObjectStoreClient::download_files(): " + responses.front().first_line() );
                }
                else {
                  const size_t response_index = first_file_idx + response_count * thread_count;
                  const string & filename = download_requests.at( response_index ).filename.string();

                  roost::atomic_create( responses.front().body(), filename,
                                        download_requests[ response_index ].mode.initialized(),
                                        download_requests[ response_index ].mode.get_or( 0 ) );

                  success_callback( download_requests[ response_index ] );
                }

                responses.pop();
                response_count++;
              }
            }
          }
        }, thread_index
      );
    }
  }

  for ( auto & thread : threads ) {
    thread.join();
  }
}