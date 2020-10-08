#include "tasksys.h"
#include <iostream>


IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}

ITaskSystem::~ITaskSystem() {}

/*
 * ================================================================
 * Serial task system implementation
 * ================================================================
 */

const char *TaskSystemSerial::name() {
    return "Serial";
}

TaskSystemSerial::TaskSystemSerial(int num_threads) : ITaskSystem(num_threads) {
}

TaskSystemSerial::~TaskSystemSerial() {}

void TaskSystemSerial::run(IRunnable *runnable, int num_total_tasks) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                                          const std::vector<TaskID> &deps) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemSerial::sync() {
    return;
}

/*
 * ================================================================
 * Parallel Task System Implementation
 * ================================================================
 */

const char *TaskSystemParallelSpawn::name() {
    return "Parallel + Always Spawn";
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads) : ITaskSystem(num_threads) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable *runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                                                 const std::vector<TaskID> &deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Spinning Task System Implementation
 * ================================================================
 */

const char *TaskSystemParallelThreadPoolSpinning::name() {
    return "Parallel + Thread Pool + Spin";
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads) : ITaskSystem(num_threads) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable *runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                                                              const std::vector<TaskID> &deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */

const char *TaskSystemParallelThreadPoolSleeping::name() {
    return "Parallel + Thread Pool + Sleep";
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads)
        : ITaskSystem(num_threads), total_threads(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    //
    // TODO: CS149 student implementations may decide to perform cleanup
    // operations (such as thread pool shutdown construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    done.store(true);
    new_task_cv.notify_all();
    one_task_done_cv.notify_all();
    for (auto &td: worker_threads_v) {
        td.join();
    }
    dispatch_thread.join();
}

void TaskSystemParallelThreadPoolSleeping::init_workers() {
    if (!worker_threads_v.empty()) {
        return;
    }

    for (auto i = 0; i < total_threads; ++i) {
        worker_threads_v.emplace_back(std::thread([this]() {
            Task task(nullptr, -1, -1, -1);
            bool new_task{false};
            while (true) {
                if (done) break;
                {
                    std::lock_guard<std::mutex> tq_lock(this->tq_mtx);
                    if (!this->ok_to_run_task_queue.empty()) {
                        const auto &first = this->ok_to_run_task_queue.front();
                        if (first.p_irunnable == nullptr || first.num_total_tasks == -1 || first.task_id == -1) {
                            continue;
                        }
                        task = first;
                        this->ok_to_run_task_queue.pop();
                        new_task = true;
                    }
                }
                if (new_task) {
                    task.p_irunnable->runTask(task.task_id, task.num_total_tasks);
                    new_task = false;
                    std::lock_guard<std::mutex> tq_lock(this->pending_mtx);
                    std::cout << "runTask async_id=" << task.async_task_id << " task_id=" << task.task_id << std::endl;
                    task_left_map[task.async_task_id] -= 1;
                    this->one_task_done_cv.notify_all();
                } else {
                    std::unique_lock<std::mutex> lk(this->new_task_mtx);
                    this->new_task_cv.wait(lk);
                }
            }
        }));
    }
}

void TaskSystemParallelThreadPoolSleeping::init_dispatch() {

    if (dispatch_init) return;

    dispatch_thread = std::thread([this]() {

        while (true) {
            if (done) break;
            bool ok_to_add{true};

            {
                std::lock_guard<std::mutex> lk(pending_mtx);
                for (auto it = pending_task_map.cbegin(); it != pending_task_map.cend();) {
                    const auto &async_task_id = it->first;
                    const auto &pending_task = it->second;
                    for (const auto &dep_async_tsk_id: pending_task.deps) {
                        if (task_left_map.count(dep_async_tsk_id)) {
                            if (task_left_map[dep_async_tsk_id].load() != 0) {
                                ok_to_add = false;
                                break;
                            }
                        }
                    }

                    if (ok_to_add) {
                        // ready to add to working queue
                        std::lock_guard<std::mutex> lk_add(tq_mtx);
                        for (const auto &tsk: pending_task.task_to_add) {
                            ok_to_run_task_queue.push(tsk);
                        }
                        std::cout << "dispatching async_task_id=" << async_task_id << " size="
                                  << pending_task.task_to_add.size() << std::endl;
                        pending_task_map.erase(it++); // remove from pending task map after insert
                        this->new_task_cv.notify_all();
                    } else {
                        ++it;
                    }
                }
            }

            if (!ok_to_add) {
                std::unique_lock<std::mutex> lk(this->task_done_mtx);
                this->one_task_done_cv.wait(lk);  // block until new task is finished
            }
        }
    });

    dispatch_init = true;

}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable *runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Parts A and B.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    init_workers();
    init_dispatch();
    runAsyncWithDeps(runnable, num_total_tasks, {});
    sync();
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                                                              const std::vector<TaskID> &deps) {


    //
    // TODO: CS149 students will implement this method in Part B.
    //

    int async_task_id = total_async_task_id.fetch_add(1);

    auto task_to_add = std::vector<Task>();
    task_to_add.reserve(num_total_tasks);
    for (int i = 0; i < num_total_tasks; i++) {
        task_to_add.emplace_back(runnable, async_task_id, i, num_total_tasks);
    }

    PendingTask pt;
    pt.deps = deps;
    pt.task_to_add = task_to_add;

    {
        std::lock_guard<std::mutex> lk(pending_mtx);
        pending_task_map[async_task_id] = pt;
        task_left_map[async_task_id] = num_total_tasks;
    }

    return async_task_id;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //

    while (true) {
        if (task_left_map.empty()) return;
        bool sync_done{true};

        {
            std::lock_guard<std::mutex> lk(tq_mtx);
            if (!ok_to_run_task_queue.empty()) {
                sync_done = false;
                std::cout << "1 ok_to_run_task_queue not empty" << std::endl;
            }
        }

        {
            std::lock_guard<std::mutex> lk(pending_mtx);
            if (!pending_task_map.empty()) {
                sync_done = false;
                std::cout << "2 pending_task_map not empty" << std::endl;
            }
            for (const auto &tl: task_left_map) {
                if (tl.second.load() != 0) {
                    sync_done = false;
                    std::cout << "tl.second.load()=" << tl.second.load() << " breaking"<< std::endl;
                    break;
                }
            }
        }

        if (sync_done) {
            return;
        } else {
            std::unique_lock<std::mutex> lk(task_done_mtx);
            this->one_task_done_cv.wait(lk);  // block until new task is finished
            std::cout << "main checking" << std::endl;
        }
    }
}


