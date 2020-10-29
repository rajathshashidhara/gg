/* -*-mode:c++; tab-width: 2; indent-tabs-mode: nil; c-basic-offset: 2 -*- */

#include <map>
#include <string>
#include <memory>
#include <stdexcept>

#include "net/http_request.hh"
#include "net/http_response.hh"
#include "net/http_request_parser.hh"
#include "execution/loop.hh"
#include "thunk/ggutils.hh"
#include "util/path.hh"

using namespace std;

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

void usage( char * argv0 )
{
  cerr << argv0 << " IP PORT" << endl;
}

int main( int argc, char * argv[] )
{
  try {
    if ( argc <= 0 ) {
      abort();
    }

    if ( argc != 3 ) {
      usage( argv[ 0 ] );
      return EXIT_FAILURE;
    }

    int port_argv = stoi( argv[ 2 ] );

    if ( port_argv <= 0 or port_argv > numeric_limits<uint16_t>::max() ) {
      throw runtime_error( "invalid port" );
    }

    Address listen_addr { argv[ 1 ], static_cast<uint16_t>( port_argv ) };
    ExecutionLoop exec_loop;

    exec_loop.make_listener( listen_addr,
      [] ( ExecutionLoop & loop, TCPSocket && socket ) -> bool {
        auto request_parser = make_shared<HTTPRequestParser>();

        auto connection = loop.add_connection<TCPSocket>( move( socket ),
          [request_parser] ( shared_ptr<TCPConnection> connection, string && data ) {
            request_parser->parse( data );

            while ( not request_parser->empty() ) {
              HTTPRequest request { move( request_parser->front() ) };
              request_parser->pop();

              const string & first_line = request.first_line();
              const string::size_type first_space = first_line.find( ' ' );
              const string::size_type last_space = first_line.rfind( ' ' );

              if ( first_space == string::npos or last_space == string::npos ) {
                /* wrong http request */
                connection->enqueue_write( get_canned_response( 400, request ) );
                continue;
              }

              if ( first_line.substr( 0, first_space ) == "GET" ) {
                const string requested_object = first_line.substr( first_space + 2,
                                                                  last_space - first_space - 2 );

                const roost::path object_path = gg::paths::blob( requested_object );
                if ( not roost::exists( object_path ) or
                    roost::is_directory( object_path ) or
                    requested_object.find( '/' ) != string::npos ) {
                  connection->enqueue_write( get_canned_response( 404, request ) );
                  continue;
                }

                const string payload = roost::read_file( object_path );
                HTTPResponse response;
                response.set_request( request );
                response.set_first_line( "HTTP/1.1 200 OK" );
                response.add_header( HTTPHeader{ "Content-Length", to_string( payload.size() ) } );
                response.add_header( HTTPHeader{ "Content-Type", "application/octet-stream" } );
                response.done_with_headers();
                response.read_in_body( payload );
                assert( response.state() == COMPLETE );

                connection->enqueue_write( response.str() );
                cerr << "served " << requested_object << endl;
              } else if (first_line.substr( 0, first_space ) == "POST" ) {
                const string requested_object = first_line.substr( first_space + 2,
                                                                  last_space - first_space - 2 );

                const roost::path object_path = gg::paths::blob( requested_object );
                cerr << object_path.string() << endl;
                if ( requested_object.find('/') != string::npos ) {
                  connection->enqueue_write( get_canned_response( 400, request ) );
                  continue;
                }

                if ( roost::exists( object_path ) and roost::is_directory( object_path )) {
                  cerr << "path: " << requested_object << " is a directory." << endl;
                  connection->enqueue_write( get_canned_response( 400, request ) );
                  continue;
                }

                if ( roost::exists( object_path ) and
                    (size_t) roost::file_size( object_path ) != request.body().size()) {
                  cerr << "file: " << requested_object << " already exists with size: " << roost::file_size( object_path )
                      << " request_body_size: " << request.body().size() << endl;
                  connection->enqueue_write( get_canned_response( 400, request ) );
                  continue;
                }

                /* assuming we only upload thunks here! */
                roost::atomic_create(request.body(), object_path, true, 0500);

                HTTPResponse response;
                const string payload = "";
                response.set_request( request );
                response.set_first_line( "HTTP/1.1 200 OK" );
                response.add_header( HTTPHeader{ "Content-Length", to_string( payload.size() ) } );
                response.add_header( HTTPHeader{ "Content-Type", "application/octet-stream" } );
                response.done_with_headers();
                response.read_in_body( payload );
                assert( response.state() == COMPLETE );

                connection->enqueue_write( response.str() );
                cerr << "served " << requested_object << endl;
              } else {
                /* only GET/PUT requests are supported */
                connection->enqueue_write( get_canned_response( 405, request ) );
                continue;
              }
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
          }
        );

        return true;
      } );

    while ( true ) {
      exec_loop.loop_once( -1 );
    }
  }
  catch ( const exception &  e ) {
    print_exception( argv[ 0 ], e );
    return EXIT_FAILURE;
  }
}
