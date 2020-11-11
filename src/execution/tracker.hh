/* -*-mode:c++; tab-width: 2; indent-tabs-mode: nil; c-basic-offset: 2 -*- */

#ifndef TRACKER_HH_
#define TRACKER_HH_

#include <string>
#include <vector>
#include <deque>
#include <memory>
#include <unordered_set>
#include <chrono>

#include "scheduler.hh"
#include "thunk/graph.hh"

class Scheduler;

class Tracker
{
private:
  const std::string target_hash_;
  std::unordered_set<std::string> remaining_targets_ {};
  ExecutionGraph dep_graph_ {};

  std::deque<std::string> job_queue_ {};
  std::unordered_set<std::string> running_jobs_ {};
  size_t finished_jobs_ { 0 };
  float estimated_cost_ { 0.0 };

  void finalize_execution( const std::string & old_hash,
                           std::vector<gg::ThunkOutput> && outputs,
                           const float cost = 0.0 );

  friend class Scheduler;
public:
  Tracker( const std::string & target_hashes );

  /* Target hash */
  std::string target_hash() const { return target_hash_; }

  /* Outputs next job */
  std::string next();

  /* Check if execution is complete */
  bool is_finished() const { return ( remaining_targets_.size() == 0 ); }

  /* Get reduction result */
  std::vector<std::string> reduce();

  /* Get status */
  void print_status() const;
};

#endif /* TRACKER_HH_ */