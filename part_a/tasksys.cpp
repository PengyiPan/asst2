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

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads) : ITaskSystem(num_threads),
                                                                    total_threads(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //

}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {

}

void TaskSystemParallelSpawn::run(IRunnable *runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

//    for (int i = 0; i < num_total_tasks; i++) {
//        runnable->runTask(i, num_total_tasks);
//    }

    auto tsk_per_td = num_total_tasks / total_threads;
    for (auto i = 0; i < total_threads; ++i) {
        auto tsk_start = i * tsk_per_td;
        auto tsk_end = i == total_threads - 1 ? num_total_tasks : tsk_start + tsk_per_td;
        this->worker_threads_v.emplace_back(std::thread([tsk_start, tsk_end, num_total_tasks, &runnable]() {
            for (auto task_id = tsk_start; task_id < tsk_end; task_id++) {
                runnable->runTask(task_id, num_total_tasks);
            }
        }));
    }

    for (auto &td: this->worker_threads_v) {
        if(td.joinable()) td.join();
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                                                 const std::vector<TaskID> &deps) {
    return 0;
}

void TaskSystemParallelSpawn::sync() {
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

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads)
        : ITaskSystem(num_threads), total_threads(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //

}

void TaskSystemParallelThreadPoolSpinning::init_workers() {
    if (!worker_threads_v.empty()) {
        return;
    }

    for (auto i = 0; i < total_threads; ++i) {
        worker_threads_v.emplace_back(std::thread([this]() {
            Task task(nullptr, 0, 0);
            bool new_task{false};
            while (true) {
                {
                    if (done) break;

                    std::lock_guard<std::mutex> tq_lock(this->tq_mtx);
                    if (!this->task_queue.empty()) {
                        const auto &first = this->task_queue.front();
                        if (first.p_irunnable == nullptr || first.num_total_tasks == -1 || first.task_id == -1) {
                            continue;
                        }
                        task = first;
                        this->thread_in_work++;
                        this->task_queue.pop();
                        new_task = true;
                    }
                }
                if (new_task) {
                    task.p_irunnable->runTask(task.task_id, task.num_total_tasks);
                    new_task = false;
                    this->thread_in_work--;
                }
            }
        }));
    }

}


TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {
    done.store(true);
    for (auto &td: worker_threads_v) {
        td.join();
    }
}


void TaskSystemParallelThreadPoolSpinning::run(IRunnable *runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //
//
//    for (int i = 0; i < num_total_tasks; i++) {
//        runnable->runTask(i, num_total_tasks);
//    }

    init_workers();

    {
        std::lock_guard<std::mutex> tq_lock(tq_mtx);
        for (int i = 0; i < num_total_tasks; i++) {
            task_queue.push(Task(runnable, i, num_total_tasks));
        }
    }

    while (true) {
        if (thread_in_work.load() == 0) {
            std::lock_guard<std::mutex> tq_lock(tq_mtx);
            if (task_queue.empty()) {
                break;
            }
        }
    }

}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                                                              const std::vector<TaskID> &deps) {
    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
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
    data_cv.notify_all();
    for (auto &td: worker_threads_v) {
        td.join();
    }
}

void TaskSystemParallelThreadPoolSleeping::init_workers() {
    if (!worker_threads_v.empty()) {
        return;
    }

    for (auto i = 0; i < total_threads; ++i) {
        worker_threads_v.emplace_back(std::thread([this]() {
            Task task(nullptr, 0, 0);
            bool new_task{false};
            while (true) {
                if (done) break;
                {
                    std::lock_guard<std::mutex> tq_lock(this->tq_mtx);
                    if (!this->task_queue.empty()) {
                        const auto &first = this->task_queue.front();
                        if (first.p_irunnable == nullptr || first.num_total_tasks == -1 || first.task_id == -1) {
                            continue;
                        }
                        task = first;
                        this->thread_in_work++;
                        this->task_queue.pop();
                        new_task = true;
                    }
                }
                if (new_task) {
                    task.p_irunnable->runTask(task.task_id, task.num_total_tasks);
                    new_task = false;
                    this->thread_in_work--;
                } else {
                    if (thread_in_work == 0) {
                        this->done_cv.notify_all();
                    }
                    std::unique_lock<std::mutex> data_lk(this->data_cv_mtx);
                    this->data_cv.wait(data_lk);
                }
            }
        }));
    }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable *runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Parts A and B.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

//    for (int i = 0; i < num_total_tasks; i++) {
//        runnable->runTask(i, num_total_tasks);
//    }

    init_workers();

    {
        std::lock_guard<std::mutex> tq_lock(tq_mtx);
        for (int i = 0; i < num_total_tasks; i++) {
            task_queue.push(Task(runnable, i, num_total_tasks));
        }
    }
    data_cv.notify_all();

    std::unique_lock<std::mutex> done_lk(tq_mtx);
    done_cv.wait(done_lk, [this]{return this->task_queue.empty();});

}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                                                              const std::vector<TaskID> &deps) {


    //
    // TODO: CS149 students will implement this method in Part B.
    //

    return 0;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //

    return;
}


