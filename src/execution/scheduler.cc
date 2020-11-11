/* -*-mode:c++; tab-width: 2; indent-tabs-mode: nil; c-basic-offset: 2 -*- */

#include "scheduler.hh"

#include <iomanip>
#include <sstream>
#include <iostream>
#include <cmath>
#include <numeric>
#include <chrono>

#include "thunk/ggutils.hh"
#include "thunk/thunk_reader.hh"
#include "net/s3.hh"
#include "tui/status_bar.hh"
#include "util/optional.hh"
#include "util/exception.hh"
#include "util/timeit.hh"
#include "util/path.hh"
#include "util/digest.hh"

using namespace std;
using namespace gg;
using namespace gg::thunk;
using namespace std::chrono;

using ReductionResult = gg::cache::ReductionResult;

size_t Scheduler::remaining_jobs() const
{
  size_t jobs = 0;

  for (auto & tracker : pending_dags_)
  {
    jobs += tracker->dep_graph_.size();
  }

  return jobs;
}

void Scheduler::print_status() const
{
  if (not status_bar_)
    return;

  static time_point<steady_clock> last_display = steady_clock::now();

  const auto this_display = steady_clock::now();
  if ( duration_cast<milliseconds>( this_display - last_display ).count() > 33 ) {
    last_display = this_display;

    stringstream data;

    data << "dags: " << setw( 5 ) << left << pending_dags_.size();
    data << "in queue: " << setw( 5 ) << left << job_queue_.size();

    for ( auto & ee : exec_engines_ ) {
      data << " " << ee->label() << " (" << ee->max_jobs() << "): "
           << setw( 5 ) << left << ee->job_count();
    }

    data << " done: "  << setw( 5 ) << left
         << finished_jobs_
         << " remaining: " << remaining_jobs();

    data << "  |  cost: " << "~$" << setw( 8 ) << fixed
         << setprecision( 2 ) << estimated_cost_;

    /* Print to STDOUT */
    cout << data.str() << endl;
  }
}

static void print_gg_message( const string & tag, const string & message )
{
  cerr << "[" << tag << "] " << message << endl;
}

Scheduler::Scheduler( std::vector<std::unique_ptr<ExecutionEngine>> && execution_engines,
             std::vector<std::unique_ptr<ExecutionEngine>> && fallback_engines,
             std::unique_ptr<StorageBackend> && storage_backend,
             const std::chrono::milliseconds default_timeout,
             const size_t timeout_multiplier,
             const bool status_bar,
             const size_t queue_lookahead,
             const PlacementHeuristic heuristic)
  : status_bar_( status_bar ),
    lookahead_( queue_lookahead ),
    heuristic_( heuristic ),
    default_timeout_( default_timeout ),
    timeout_multiplier_( timeout_multiplier ),
    exec_engines_( move( execution_engines ) ),
    fallback_engines_( move( fallback_engines ) ),
    storage_backend_( move( storage_backend ) )
{
  auto success_callback =
    [this] ( const string & old_hash, vector<ThunkOutput> && outputs, const float cost )
    { finalize_execution( old_hash, move( outputs ), cost ); };

  auto failure_callback =
    [this] ( const string & old_hash, const JobStatus failure_reason )
    {
      switch ( failure_reason ) {
      /* this is the only fatal failure */
      case JobStatus::ExecutionFailure:
        throw runtime_error( "execution failed: " + old_hash );

      /* for all of the following cases, except default, we will push the failed
      job back into the queue */
      case JobStatus::InvocationFailure:
        print_gg_message( "warning", "invocation failed: " + old_hash );
        break;

      case JobStatus::RateLimit:
        print_gg_message( "warning", "rate limited: " + old_hash );
        break;

      case JobStatus::FetchDependenciesFailure:
        print_gg_message( "warning", "fetching the dependencies failed: " + old_hash );
        break;

      case JobStatus::UploadOutputFailure:
        print_gg_message( "warning", "uploading the output failed: " + old_hash );
        break;

      case JobStatus::OperationalFailure:
        print_gg_message( "warning", "operational failure: " + old_hash );
        break;

      case JobStatus::SocketFailure:
        print_gg_message( "warning", "socket failure: " + old_hash );
        break;

      case JobStatus::ChildProcessFailure:
        print_gg_message( "warning", "child process failure: " + old_hash );
        break;

      default:
        throw runtime_error( "execution failed for an unknown reason: " + old_hash );
      }

      /* let's retry */
      auto it = running_jobs_.find(old_hash);
      if (it == running_jobs_.end())
        throw runtime_error( "inconsistent state" );

      std::shared_ptr<Tracker> tracker = it->second.tracker_;
      tracker->job_queue_.push_front(old_hash);
    };

  if ( exec_engines_.size() == 0 ) {
    throw runtime_error( "no execution engines are available" );
  }

  for ( auto & ee : exec_engines_ ) {
    ee->set_success_callback( success_callback );
    ee->set_failure_callback( failure_callback );
    ee->init( exec_loop_ );
  }

  for ( auto & fe : fallback_engines_ ) {
    fe->set_success_callback( success_callback );
    fe->set_failure_callback( failure_callback );
    fe->init( exec_loop_ );
  }
}

void Scheduler::finalize_execution( const string & old_hash,
                                   vector<ThunkOutput> && outputs,
                                   const float cost )
{
  auto it = running_jobs_.find( old_hash );
  if (it == running_jobs_.end())
    throw runtime_error( "inconsistent state" );

  std::shared_ptr<Tracker> tracker = it->second.tracker_;
  tracker->finalize_execution(old_hash, move(outputs), cost);

  running_jobs_.erase(old_hash);
}

void Scheduler::add_dag( const std::vector<std::string> & target_hashes )
{
  for (auto & hash : target_hashes)
  {
    shared_ptr<Tracker> dag = make_shared<Tracker>(hash);
    pending_dags_.push_back(dag);
  }
}

vector<shared_ptr<Tracker>> Scheduler::run_once()
{
  vector<shared_ptr<Tracker>> finished_dags;
  for (auto it = pending_dags_.begin(); it != pending_dags_.end();)
  {
    shared_ptr<Tracker> dag = *it;
    if (dag->is_finished()) {
      finished_dags.push_back(dag);

      it = pending_dags_.erase(it);
    } else {
      while (true) {
        string hash = dag->next();

        if (hash.empty())
          break;

        job_queue_.emplace_back(hash, dag);
      }

      it++;
    }
  }

  print_status();

  /* Issue retries */
  const auto poll_result = exec_loop_.loop_once( timeout_check_interval_ == 0s
                                                  ? -1
                                                  : timeout_check_interval_.count() );
  const auto clock_now = Clock::now();

  if ( timeout_check_interval_ != 0s and clock_now >= next_timeout_check_ ) {
    size_t count = 0;

    for ( auto & job : running_jobs_ ) {
      if ( job.second.timeout != 0ms and
            ( clock_now - job.second.start ) > job.second.timeout ) {
        job_queue_.emplace_back( job.first, job.second.tracker_ );
        job.second.start = clock_now;
        job.second.timeout += job.second.restarts * job.second.timeout;
        job.second.restarts++;

        count ++;
      }
    }

    next_timeout_check_ += timeout_check_interval_;

    if ( count > 0 ) {
      print_gg_message( "info", "duplicating " + to_string( count ) +
                                " job" + ( ( count == 1 ) ? "" : "s" ) );
    }
  }

  if (poll_result.result == Poller::Result::Type::Exit) {
    throw runtime_error( "unhandled poller failure happened, job is not finished" );
  }

  if (job_queue_.empty())
    return finished_dags;

  auto result = pick_job();
  if (!result.initialized()) {
    return finished_dags;
  }

  auto schedule_info = result.get();
  string thunk_hash = schedule_info.first->first;
  std::shared_ptr<Tracker> dag = schedule_info.first->second;
  job_queue_.erase(schedule_info.first);

  /* don't bother executing gg-execute if it's in the cache */
  Optional<ReductionResult> cache_entry;

  while ( true ) {
    auto temp_cache_entry = gg::cache::check( cache_entry.initialized() ? cache_entry->hash
                                                                        : thunk_hash );

    if ( temp_cache_entry.initialized() ) {
      cache_entry = move( temp_cache_entry );
    }
    else {
      break;
    }
  }

  if (cache_entry.initialized()) {
    Thunk thunk { ThunkReader::read( gg::paths::blob( thunk_hash ), thunk_hash ) };
    vector<ThunkOutput> new_outputs;

    for ( const auto & tag : thunk.outputs() ) {
      Optional<cache::ReductionResult> result = cache::check( gg::hash::for_output( thunk_hash, tag ) );

      if ( not result.initialized() ) {
        throw runtime_error( "inconsistent cache entries" );
      }

      new_outputs.emplace_back( result->hash, tag );
    }

    finalize_execution( thunk_hash, move( new_outputs ), 0 );
  }
  else {
    const Thunk & thunk = dag->dep_graph_.get_thunk( thunk_hash );
    std::unique_ptr<ExecutionEngine> &engine = schedule_info.second;
    engine->force_thunk( thunk, exec_loop_ );

    auto it = running_jobs_.find( thunk_hash );
    if (it == running_jobs_.end()) {
      JobInfo job_info(dag);

      job_info.start = Clock::now();
      job_info.timeout = thunk.timeout() * timeout_multiplier_;
      job_info.restarts++;

      if ( job_info.timeout == 0s ) {
        job_info.timeout = default_timeout_;
      }

      running_jobs_.insert( make_pair(thunk_hash, move(job_info) ) );
    } else {
      JobInfo & job_info = it->second;

      job_info.start = Clock::now();
      job_info.timeout = thunk.timeout() * timeout_multiplier_;
      job_info.restarts++;

      if ( job_info.timeout == 0s ) {
        job_info.timeout = default_timeout_;
      }
    }
  }

  return finished_dags;
}

typedef std::list<std::pair<std::string, std::shared_ptr<Tracker>>>::iterator job_iterator;

Optional<pair<job_iterator, std::unique_ptr<ExecutionEngine>&>> Scheduler::pick_job()
{
  Optional<pair<job_iterator, std::unique_ptr<ExecutionEngine>&>> result;

  switch (heuristic_)
  {
  case PlacementHeuristic::First:
    for (auto thunk_it = job_queue_.begin(); thunk_it != job_queue_.end(); thunk_it++)
    {
      std::shared_ptr<Tracker> dag = thunk_it->second;
      const Thunk& thunk = dag->dep_graph_.get_thunk(thunk_it->first);

      for (auto & exec_engine : exec_engines_)
      {
        if (exec_engine->is_remote() && thunk.is_localonly())
          continue;

        if (exec_engine->can_execute(thunk)) {
          if (exec_engine->job_count() >= exec_engine->max_jobs())
            continue;

          result.initialize(thunk_it, exec_engine);
          return result;
        }
      }
    }

    /* Try fallback engines! */
    for (auto thunk_it = job_queue_.begin(); thunk_it != job_queue_.end(); thunk_it++)
    {
      std::shared_ptr<Tracker> dag = thunk_it->second;
      const Thunk& thunk = dag->dep_graph_.get_thunk(thunk_it->first);

      for (auto & exec_engine : fallback_engines_)
      {
        if (exec_engine->is_remote() && thunk.is_localonly())
          continue;

        if (exec_engine->can_execute(thunk)) {
          if (exec_engine->job_count() >= exec_engine->max_jobs())
            continue;

          result.initialize(thunk_it, exec_engine);
          return result;
        }
      }
    }
    break;

  default:
    throw runtime_error("invalid placement heuristic");
  }

  return result;
}