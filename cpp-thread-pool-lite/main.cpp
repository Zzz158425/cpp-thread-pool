#include "cpp-thread-pool-lite.h"

int sum1(int a, int b)
{
    std::this_thread::sleep_for(std::chrono::seconds(3));
    return a + b;
}
int sum2(int a, int b, int c)
{
    std::this_thread::sleep_for(std::chrono::seconds(3));
    return a + b + c;
}

int main()
{
    // packaged_task 包装并执行任务，future 负责在将来获取这个任务的返回值
    // packaged_task + future 基本替代了原来手写的 Task + Result + Any + Semaphore 这一整套返回值机制
    // std::packaged_task ≈ Task + Result::setVal() 的一部分
    // std::future ≈ Result::get() + Semaphore + Any 的一部分
    std::packaged_task<int(int, int)> task(sum1);
    std::future<int> res = task.get_future();// future <=> Result
    std::thread t(std::move(task), 10, 20);// packaged_task 无左值的拷贝构造与 operator= 重载
    t.detach();
    std::cout << res.get() << std::endl;// Semaphore <=> res.get() 会阻塞，直到任务执行完返回结果

    ThreadPool pool;
    // pool.setMode(PoolMode::MODE_CACHED);
    pool.start(2);

    std::future<int> r1 = pool.submitTask(sum1, 1, 2);
    std::future<int> r2 = pool.submitTask(sum2, 1, 2, 3);
    std::future<int> r3 = pool.submitTask([](int& b, int& e)-> int 
        {
            int sum = 0;
            for (int i = b; i <= e; i++)
            {
                sum += i;
            }
            return sum; 
        }, 1, 100);
    std::future<int> r4 = pool.submitTask(sum1, 1, 2);
    std::future<int> r5 = pool.submitTask(sum1, 1, 2);

    std::cout << r1.get() << std::endl;
    std::cout << r2.get() << std::endl;
    std::cout << r3.get() << std::endl;
    std::cout << r4.get() << std::endl;
    std::cout << r5.get() << std::endl;

    return 0;
}