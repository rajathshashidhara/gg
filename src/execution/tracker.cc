/* -*-mode:c++; tab-width: 2; indent-tabs-mode: nil; c-basic-offset: 2 -*- */

#include "tracker.hh"

#include <iomanip>
#include <sstream>
#include <iostream>
#include <cmath>
#include <numeric>
#include <chrono>

#include "thunk/ggutils.hh"
#include "thunk/thunk_reader.hh"
#include "util/optional.hh"
#include "util/exception.hh"
#include "util/timeit.hh"
#include "util/path.hh"
#include "util/digest.hh"

using namespace std;
using namespace std::chrono;
using namespace gg;
using namespace gg::thunk;

using ReductionResult = gg::cache::ReductionResult;

void Tracker::print_status() const
{
  stringstream ss;
  ss << "Completed: " << target_hash_ << endl;
  cout << ss.str();
}

Tracker::Tracker( const std::string & target_hashes )
  : target_hash_(target_hashes)
{
  remaining_targets_.insert(target_hash_);

  cerr << "\u2192 Loading the thunks... ";
  auto graph_load_time = time_it<milliseconds>(
    [this] ()
    {
      dep_graph_.add_thunk( target_hash_ );

      unordered_set<string> thunk_o1_deps = dep_graph_.order_one_dependencies( target_hash_ );
      job_queue_.insert( job_queue_.end(), thunk_o1_deps.begin(), thunk_o1_deps.end() );
    } ).count();
  cerr << " done (" << graph_load_time << " ms)." << endl;
}

void Tracker::finalize_execution( const string & old_hash,
                                   vector<ThunkOutput> && outputs,
                                   const float cost )
{
  running_jobs_.erase( old_hash );
  const string main_output_hash = outputs.at( 0 ).hash;

  Optional<unordered_set<string>> new_o1s = dep_graph_.force_thunk( old_hash, move ( outputs ) );
  estimated_cost_ += cost;

  if ( new_o1s.initialized() ) {
    job_queue_.insert( job_queue_.end(), new_o1s->begin(), new_o1s->end() );

    if ( gg::hash::type( main_output_hash ) == gg::ObjectType::Value ) {
      remaining_targets_.erase( dep_graph_.original_hash( old_hash ) );
    }

    finished_jobs_++;
  }
}

string Tracker::next()
{
  string job = job_queue_.front();
  running_jobs_.insert( job );
  job_queue_.pop_front();

  return job;
}

vector<string> Tracker::reduce()
{
  if (not is_finished())
  {
    throw runtime_error( "unhandled poller failure happened, job is not finished" );
  }

  vector<string> final_hashes;

  const string final_hash = dep_graph_.updated_hash( target_hash_ );
  const Optional<ReductionResult> answer = gg::cache::check( final_hash );
  if ( not answer.initialized() ) {
    throw runtime_error( "internal error: final answer not found for " + target_hash_ );
  }
  final_hashes.emplace_back( answer->hash );

  return final_hashes;
}
