// ============ thread_pool.h ============
#ifndef THREAD_POOL_H
#define THREAD_POOL_H

#include <iostream>
#include <queue>
#include <vector>
#include <pthread.h>
#include <sys/socket.h> // recv, send
#include <unistd.h>     // close
#include <errno.h>      // errno, EAGAIN, EWOULDBLOCK
#include <atomic>
extern int g_epfd;
extern void handle_client(int fd);
// 线程池类：管理固定数量的工作线程
class ThreadPool
{
public:
    // 构造函数：创建 numThreads 个工作线程
    // 调用时机：main 函数里，epoll 初始化之后，主循环之前
    ThreadPool(size_t numThreads) : stop(false), active_workers(0)
    {
        pthread_mutex_init(&mutex, nullptr); //
        pthread_cond_init(&cond, nullptr);   //

        // 创建 numThreads 个线程，全部进入 worker 函数
        for (size_t i = 0; i < numThreads; i++)
        {
            pthread_t pid;
            pthread_create(&pid, nullptr, worker, this);
            pthread_detach(pid); // 分离线程，自动回收
        }
    }

    // 析构函数：通知所有线程退出
    ~ThreadPool()
    {
        pthread_mutex_lock(&mutex);
        stop = true; // 设置停止标志
        pthread_mutex_unlock(&mutex);

        // 唤醒所有线程，让它们检查 stop 后退出
        pthread_cond_broadcast(&cond);
        // 等所有 worker 真正退出后再销毁 mutex/cond
        // 不然线程还在用 mutex，你这边就 destroy
        while (active_workers > 0)
        {
            usleep(1000); // 1ms 轮询一次
        }
        pthread_mutex_destroy(&mutex);
        pthread_cond_destroy(&cond);
    }

    // 主线程调用：把任务（client_fd）加入队列
    // 调用时机：epoll_wait 发现 client_fd 有数据时

    void enqueue(int fd)
    {
        pthread_mutex_lock(&mutex);
        if (stop)
        {
            pthread_mutex_unlock(&mutex);
            return;
        }
        tasks.push(fd); // 任务入队
        pthread_mutex_unlock(&mutex);
        pthread_cond_signal(&cond); // 唤醒一个工作线程
    }

private:
    // 工作线程入口：静态函数，因为 pthread_create 要求 C 函数指针
    static void *worker(void *arg)
    {
        ThreadPool *pool = (ThreadPool *)arg;
        pool->active_workers++; // 标记自己开始工作
        while (1)
        {
            int fd = -1;
            // ========== 临界区：操作队列必须加锁 ==========
            pthread_mutex_lock(&pool->mutex);

            // while 防止虚假唤醒：被唤醒后队列可能还是空的
            while (!pool->stop && pool->tasks.empty())
            {
                pthread_cond_wait(&pool->cond, &pool->mutex);
                // 内部：释放锁 → 阻塞(睡眠) → 被唤醒后自动重新加锁
            }
            // 如果停止且队列空，退出线程
            if (pool->stop && pool->tasks.empty())
            {
                pthread_mutex_unlock(&pool->mutex);
                break;
            }
            // 取出队列头部任务
            fd = pool->tasks.front();
            pool->tasks.pop();
            pthread_mutex_unlock(&pool->mutex);
            // ========== 临界区结束 ==========

            // 执行 IO（此时已解锁，不阻塞其他线程）
            if (fd >= 0)
            {
                handle_client(fd);
            }
        }
        pool->active_workers--; // 标记自己已退出
        return nullptr;
    }

    std::queue<int> tasks;           // 任务队列：存 client_fd
    pthread_mutex_t mutex;           // 互斥锁：保护队列
    pthread_cond_t cond;             // 条件变量：空队列时休眠
    bool stop;                       // 停止标志
    std::atomic<int> active_workers; // 当前活跃 worker 数
};

#endif