// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab expandtab

#pragma once

#include <atomic>
#include <condition_variable>
#include <tuple>
#include <type_traits>
#include <boost/lockfree/queue.hpp>
#include <boost/optional.hpp>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/resource.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/alien.hh>

// std::counting_semaphore is buggy in libstdc++-11
// (https://gcc.gnu.org/bugzilla/show_bug.cgi?id=104928),
// so we switch back to the homebrew version for now.
#include "semaphore.h"
#include "crimson/common/log.h"

namespace crimson::os {

struct WorkItem {
  virtual ~WorkItem() {}
  virtual void process() = 0;
};

template<typename Func>
struct Task final : WorkItem {
  using T = std::invoke_result_t<Func>;
  using future_stored_type_t =
    std::conditional_t<std::is_void_v<T>,
                       seastar::internal::future_stored_type_t<>,
                       seastar::internal::future_stored_type_t<T>>;
  using futurator_t = seastar::futurize<T>;
public:
  explicit Task(Func&& f, seastar::alien::instance& alien, unsigned shard_id)
    : func(std::move(f))
    , alien(alien)
    , shard_id(shard_id)
  {}

  void process() override {
    try {
      if constexpr (std::is_void_v<T>) {
        func();
	std::ignore = seastar::alien::submit_to(
                    alien, shard_id, [this] {
                        promise.set_value();     // ← void case
                        return seastar::make_ready_future<>();
                    });
      } else {
	auto result = func();
	std::ignore = seastar::alien::submit_to(
                    alien, shard_id, [this, result=std::move(result)]
                    () mutable {
                        promise.set_value(std::move(result)); // ← pass result!
                        return seastar::make_ready_future<>();
                    });
      }
    } catch (...) {
      auto ex = std::current_exception();
      std::ignore = seastar::alien::submit_to(
                alien, shard_id, [this, ex] {
                    promise.set_exception(ex);
                    return seastar::make_ready_future<>();
                });
    }
  }
  typename futurator_t::type get_future() {
    return promise.get_future();
  }
private:
  Func func;
  seastar::promise<T> promise;
  seastar::alien::instance& alien;
  unsigned shard_id;
};

struct SubmitQueue {
  seastar::semaphore free_slots;
  seastar::gate pending_tasks;
  explicit SubmitQueue(size_t num_free_slots)
    : free_slots(num_free_slots)
  {}
  seastar::future<> stop() {
    return pending_tasks.close();
  }
};

struct ShardedWorkQueue {
public:
  WorkItem* pop_front(std::chrono::milliseconds& queue_max_wait) {
    if (sem.try_acquire_for(queue_max_wait)) {
      if (!is_stopping()) {
        WorkItem* work_item = nullptr;
        [[maybe_unused]] bool popped = pending.pop(work_item);
        assert(popped);
        return work_item;
      }
    }
    return nullptr;
  }
  void stop() {
    stopping = true;
    sem.release();
  }
  void push_back(WorkItem* work_item) {
    [[maybe_unused]] bool pushed = pending.push(work_item);
    assert(pushed);
    sem.release();
  }
private:
  bool is_stopping() const {
    return stopping;
  }
  std::atomic<bool> stopping = false;
  static constexpr unsigned QUEUE_SIZE = 128;
  crimson::counting_semaphore<QUEUE_SIZE> sem{0};
  boost::lockfree::queue<WorkItem*> pending{QUEUE_SIZE};
};

/// an engine for scheduling non-seastar tasks from seastar fibers
class ThreadPool {
public:
  /**
   * @param queue_sz the depth of pending queue. before a task is scheduled,
   *                 it waits in this queue. we will round this number to
   *                 multiple of the number of cores.
   * @param n_threads the number of threads in this thread pool.
   * @param cpu the CPU core to which this thread pool is assigned
   * @note each @c Task has its own crimson::thread::Condition, which possesses
   * an fd, so we should keep the size of queue under a reasonable limit.
   */
  ThreadPool(size_t n_threads, size_t queue_sz, const std::optional<seastar::resource::cpuset>& cpus);
  ~ThreadPool();
  seastar::future<> start();
  seastar::future<> stop();
  size_t size() {
    return n_threads;
  }

  static seastar::logger& thpool_logger() {
    return crimson::get_logger(ceph_subsys_osd);
  }

  template<typename Func, typename...Args>
  auto submit(int shard, Func&& func, Args&&... args) {
    thpool_logger().debug("submit start shard {}", shard);
    auto origin_shard = seastar::this_shard_id();
    auto packaged = [func=std::move(func),
                     args=std::forward_as_tuple(args...)] {
      return std::apply(std::move(func), std::move(args));
    };
    return seastar::with_gate(submit_queue.local().pending_tasks,
      [packaged=std::move(packaged), shard, origin_shard, this] {
        thpool_logger().debug("Entered gate shard {}", shard);
        return local_free_slots().wait()
          .then([packaged=std::move(packaged), shard, origin_shard, this] {
            thpool_logger().debug("got free slot shard {}", shard);
            auto task = new Task{std::move(packaged),
                                 seastar::engine().alien(),
                                 origin_shard
                                };
            auto fut = task->get_future();
            pending_queues[shard].push_back(task);
            return fut.finally([task, this] {
              thpool_logger().debug("task done");
              local_free_slots().signal();
              delete task;
            });
          });
        });
  }

  template<typename Func>
  auto submit(Func&& func) {
    return submit(::rand() % n_threads, std::forward<Func>(func));
  }

private:
  void loop(std::chrono::milliseconds queue_max_wait, size_t shard);
  bool is_stopping() const {
    return stopping.load(std::memory_order_relaxed);
  }
  static void pin(const seastar::resource::cpuset& cpus);
  static void block_sighup();
  seastar::semaphore& local_free_slots() {
    return submit_queue.local().free_slots;
  }
  ThreadPool(const ThreadPool&) = delete;
  ThreadPool& operator=(const ThreadPool&) = delete;

private:
  size_t n_threads;
  std::atomic<bool> stopping = false;
  std::vector<std::thread> threads;
  seastar::sharded<SubmitQueue> submit_queue;
  const size_t queue_size;
  std::vector<ShardedWorkQueue> pending_queues;
};

} // namespace crimson::os
