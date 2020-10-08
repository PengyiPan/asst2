#ifndef _TASKSYS_H
#define _TASKSYS_H

#include "itasksys.h"
#include <vector>
#include <thread>
#include <atomic>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <unordered_map>

/*
 * TaskSystemSerial: This class is the student's implementation of a
 * serial task execution engine.  See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
class TaskSystemSerial: public ITaskSystem {
    public:
        TaskSystemSerial(int num_threads);
        ~TaskSystemSerial();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

/*
 * TaskSystemParallelSpawn: This class is the student's implementation of a
 * parallel task execution engine that spawns threads in every run()
 * call.  See definition of ITaskSystem in itasksys.h for documentation
 * of the ITaskSystem interface.
 */
class TaskSystemParallelSpawn: public ITaskSystem {
    public:
        TaskSystemParallelSpawn(int num_threads);
        ~TaskSystemParallelSpawn();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

/*
 * TaskSystemParallelThreadPoolSpinning: This class is the student's
 * implementation of a parallel task execution engine that uses a
 * thread pool. See definition of ITaskSystem in itasksys.h for
 * documentation of the ITaskSystem interface.
 */
class TaskSystemParallelThreadPoolSpinning: public ITaskSystem {
    public:
        TaskSystemParallelThreadPoolSpinning(int num_threads);
        ~TaskSystemParallelThreadPoolSpinning();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

struct Task{
    IRunnable* p_irunnable{nullptr};
    TaskID async_task_id{-1};
    int task_id{-1};
    int num_total_tasks{-1};

    Task(IRunnable *pIrunnable, TaskID asyncTaskId, int taskId, int numTotalTasks) : p_irunnable(pIrunnable),
                                                                                     async_task_id(asyncTaskId),
                                                                                     task_id(taskId),
                                                                                     num_total_tasks(numTotalTasks) {}
};

struct PendingTask{
    std::vector<TaskID> deps;
    std::vector<Task> task_to_add;
};

/*
 * TaskSystemParallelThreadPoolSleeping: This class is the student's
 * optimized implementation of a parallel task execution engine that uses
 * a thread pool. See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
class TaskSystemParallelThreadPoolSleeping: public ITaskSystem {
    public:
        TaskSystemParallelThreadPoolSleeping(int num_threads);
        ~TaskSystemParallelThreadPoolSleeping();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();

private:

    void init_workers();
    void init_dispatch();

    std::vector<std::thread> worker_threads_v;
    std::thread dispatch_thread;
    bool dispatch_init{false};

    int total_threads{0};
    std::mutex tq_mtx{};
    std::queue<Task> ok_to_run_task_queue{};
    std::mutex pending_mtx{};
    std::unordered_map<TaskID, PendingTask> pending_task_map;
    std::unordered_map<TaskID, std::atomic<int>> task_left_map;
    std::atomic<int> total_async_task_id{0};
    std::atomic<bool> done{false};

    std::condition_variable new_task_cv;
    std::mutex new_task_mtx;

    std::mutex task_done_mtx{};
    std::condition_variable one_task_done_cv;
};

#endif
