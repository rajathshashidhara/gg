/* -*-mode:c++; tab-width: 2; indent-tabs-mode: nil; c-basic-offset: 2 -*- */

#ifndef SCHEDULER_H_
#define SCHEDULER_H_

#include <string>
#include <vector>
#include <deque>
#include <list>
#include <memory>
#include <chrono>
#include <unordered_set>
#include <unordered_map>

#include "loop.hh"
#include "engine.hh"
#include "tracker.hh"
#include "thunk/graph.hh"
#include "storage/backend.hh"

class Tracker;

enum class PlacementHeuristic
{
  First, Random, MostObjects, MostObjectsSize, LargestObject, LRU,
};

class Scheduler
{
private:
  using Clock = std::chrono::steady_clock;

  struct JobInfo
  {
    Tracker& tracker_;
    Clock::time_point start;
    std::chrono::milliseconds timeout { 0 };
    uint8_t restarts { std::numeric_limits<uint8_t>::max() };

    JobInfo(Tracker& tracker): tracker_ ( tracker ), start () {}
  };

  bool status_bar_;

  std::list<Tracker> pending_dags_ {};
  std::list<std::pair<std::string, Tracker&>> job_queue_ {};
  std::unordered_map<std::string, JobInfo> running_jobs_ {};
  size_t finished_jobs_ { 0 };
  float estimated_cost_ { 0.0 };
  size_t lookahead_ { 1 };
  PlacementHeuristic heuristic_ { PlacementHeuristic::First };

  std::chrono::milliseconds default_timeout_;
  size_t timeout_multiplier_;
  std::chrono::milliseconds timeout_check_interval_ { default_timeout_ / 2 };
  Clock::time_point next_timeout_check_ { Clock::now() + timeout_check_interval_ };

  ExecutionLoop exec_loop_ {};
  std::vector<std::unique_ptr<ExecutionEngine>> exec_engines_;
  std::vector<std::unique_ptr<ExecutionEngine>> fallback_engines_;

  std::unique_ptr<StorageBackend> storage_backend_;

  size_t remaining_jobs() const;

  void finalize_execution( const std::string & old_hash,
                           std::vector<gg::ThunkOutput> && outputs,
                           const float cost = 0.0 );

  Optional<std::pair<std::list<std::pair<std::string, Tracker&>>::iterator, std::unique_ptr<ExecutionEngine>&>> pick_job();

public:
  Scheduler( std::vector<std::unique_ptr<ExecutionEngine>> && execution_engines,
             std::vector<std::unique_ptr<ExecutionEngine>> && fallback_engines,
             std::unique_ptr<StorageBackend> && storage_backend,
             const std::chrono::milliseconds default_timeout = std::chrono::milliseconds { 0 },
             const size_t timeout_multiplier = 1,
             const bool status_bar = false,
             const size_t queue_lookahead = 1,
             const PlacementHeuristic heuristic = PlacementHeuristic::First);

  void add_dag( const std::vector<std::string> & target_hashes );
  std::vector<Tracker> run_once();  /* Run this function repeatedly */
  void print_status() const;

};

#endif /* SCHEDULER_H_ */