#include "cpp-thread-pool.h"
#include <chrono>
#include <thread>
// 2026.5.21

using ULong = unsigned long long;

class MyTask : public Task
{
public:
	MyTask(ULong begin, ULong end)
		: begin_(begin)
		, end_(end) { }
public:
	Any run()
	{	
		std::cout << "tid:" << std::this_thread::get_id()
			<< "begin!" << std::endl;

		std::this_thread::sleep_for(std::chrono::seconds(3));

		ULong sum = 0;
		for (ULong i = begin_; i <= end_; i++)
		{
			sum += i;
		}

		std::cout << "tid:" << std::this_thread::get_id()
			<< "end!" << std::endl;

		return sum;
	}
private:
	int begin_;
	int end_;
};

int main()
{
	{
		ThreadPool pool;
		//pool.setMode(PoolMode::MODE_CACHED);
		pool.start(2);
		
		// Linux 上，这些 Result 对象也是局部对象，要析构
		// 安全，ULong sum1 = res1.get().cast<ULong>();等待 Result 对应的任务运行完毕
		Result res1 = pool.submitTask(std::make_shared<MyTask>(1, 100000000));
		
		// 不安全，出作用域，局部变量 res2 析构，将会使一个生命周期已经结束的 condition_variable 对象调用 notify_all()
		// Result 析构后，result_ 这个指针变量里保存的地址值不会自动消失，也不会自动变成 nullptr
		// 所以语法上仍然可以写 result_->setVal(...)；但这个调用是错误的，属于未定义行为
		Result res2 = pool.submitTask(std::make_shared<MyTask>(100000001, 200000000));
		
		// 不安全，出此语句临时对象 Result() 析构，将会使一个生命周期已经结束的 condition_variable 对象调用 notify_all()
		// Result 析构后，result_ 这个指针变量里保存的地址值不会自动消失，也不会自动变成 nullptr
		// 所以语法上仍然可以写 result_->setVal(...)；但这个调用是错误的，属于未定义行为
		pool.submitTask(std::make_shared<MyTask>(200000001, 300000000));
		pool.submitTask(std::make_shared<MyTask>(200000001, 300000000));
		pool.submitTask(std::make_shared<MyTask>(200000001, 300000000));

		ULong sum1 = res1.get().cast<ULong>();
		std::cout << sum1 << std::endl;
	}// Result 对象析构，在 vs 下，条件变量析构会释放相应资源

	std::cout << "main over!" << std::endl;
	getchar();

	// 在 Linux 下同样的代码会出现类似死锁的问题
		// 根因不是 notify_all() 本身会阻塞，而是 Result 生命周期提前结束
		// Task 内部保存的是 Result* 裸指针，如果 Result 已经析构，Task::result_ 就变成悬空指针
		// 工作线程执行完成后仍然调用 result_->setVal(run())，因为 result_ 这个指针变量里保存的地址值不会自动消失
		// setVal() 内部调用 sem_.post()，此时 sem_ 里的 condition_variable 可能已经随着 Result 析构而失效
		// 对已经失效的 condition_variable 调用 notify_all() 属于未定义行为
		// 在 Linux 下可能表现为卡在 pthread_cond_broadcast，看起来像死锁
		// Windows 下没暴露出来只是实现差异，不代表代码安全

#if 0
	{
		ThreadPool pool;

		pool.setMode(PoolMode::MODE_CACHED);

		pool.start(4);

		Result res1 = pool.submitTask(std::make_shared<MyTask>(1, 100000000));
		Result res2 = pool.submitTask(std::make_shared<MyTask>(100000001, 200000000));
		Result res3 = pool.submitTask(std::make_shared<MyTask>(200000001, 300000000));
		pool.submitTask(std::make_shared<MyTask>(200000001, 300000000));
		pool.submitTask(std::make_shared<MyTask>(200000001, 300000000));
		pool.submitTask(std::make_shared<MyTask>(200000001, 300000000));

		ULong sum1 = res1.get().cast<ULong>();
		ULong sum2 = res2.get().cast<ULong>();
		ULong sum3 = res3.get().cast<ULong>();

		// Master - Slave 线程模型
			// Master 线程用来分解任务，然后给各个 Salve 线程分配任务
			// 等待各个 Salve 线程执行完任务，返回结果
			// Master 线程合并各个任务结果，输出
		// std::cout << sum1 << std::endl;
		std::cout << (sum1 + sum2 + sum3) << std::endl;
		ULong sum = 0;
		for (ULong i = 1; i <= 300000000; i++)
		{
			sum += i;
		}
		std::cout << sum << std::endl;
	}// 出作用域 pool 自动析构
	

	//pool.submitTask(std::make_shared<MyTask>());
	//pool.submitTask(std::make_shared<MyTask>());
	//pool.submitTask(std::make_shared<MyTask>());
	//pool.submitTask(std::make_shared<MyTask>());
	//pool.submitTask(std::make_shared<MyTask>());
	//pool.submitTask(std::make_shared<MyTask>());
	//pool.submitTask(std::make_shared<MyTask>());
	//pool.submitTask(std::make_shared<MyTask>());
	//pool.submitTask(std::make_shared<MyTask>());

	getchar();

#endif
	return 0;
}