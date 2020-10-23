#include <iostream>
#include <string>
#include <fstream>
#include <cstdlib>
#include <thread>
#include <chrono>

#include "thunk/thunk.hh"
#include "thunk/thunk_writer.hh"
#include "thunk/thunk_reader.hh"
#include "thunk/placeholder.hh"
#include "thunk/ggutils.hh"
#include "util/exception.hh"
#include "util/path.hh"
#include "util/util.hh"
#include "util/optional.hh"

using namespace std;
using namespace gg;
using namespace gg::thunk;

int main( int argc, char * argv[] )
{
  try {
    if (argc != 3) {
      cerr << "usage: delay <secs> <thunk/placeholder>" << endl;
      return EXIT_FAILURE;
    }

    const long long T = stoll( argv[1] );

    if (T < 0) {
      cerr << argv[0] << " doesn't accept negative inputs" << endl;
      return EXIT_FAILURE;
    }

    string thunk_hash;
    Optional<ThunkPlaceholder> placeholder = ThunkPlaceholder::read( argv[2] );
    if ( not placeholder.initialized() ) {
      if ( not ThunkReader::is_thunk( argv[2] )) {
        cerr << "not a thunk: " << argv[2] << endl;

        return EXIT_FAILURE;
      }

      thunk_hash = gg::hash::compute( argv[2], gg::ObjectType::Thunk );
    } else {
      thunk_hash = placeholder->content_hash();
    }

    std::this_thread::sleep_for(std::chrono::seconds(T));

    roost::path output_thunk_path = gg::paths::blob( thunk_hash );
    const Thunk output_thunk = ThunkReader::read(output_thunk_path, thunk_hash);

    ThunkWriter::write( output_thunk, "out" );
  }
  catch( const exception & e ) {
    print_exception( argv[ 0 ], e );
    return EXIT_FAILURE;
  }

  return EXIT_SUCCESS;
}