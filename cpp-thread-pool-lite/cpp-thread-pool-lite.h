// 防止同一个头文件被重复包含，导致重复定义或编译错误
#ifndef THREADPOOL_H// 如果还没有定义过 THREADPOOL_H，就继续往下编译
#define THREADPOOL_H

#include <vector>
#include <queue>
#include <memory>
#include <atomic>
#include <mutex>
#include <condition_variable>
#include <functional>
#include <thread>
#include <iostream>
#include <chrono>
#include <unordered_map>
#include <future>

const int TASK_MAX_THRESHHOLD = INT32_MAX;
const int THREAD_MAX_THRESHHOLD = 100;
const int THREAD_MAX_IDLE_TIME = 60;// 单位秒

// 线程池支持的模式
// 添加 class PoolMode ，防止因枚举类型不同，但是枚举项相同，造成枚举名字冲突
enum class PoolMode
{
	MODE_FIXED,// 固定数量的线程
	MODE_CACHED,// 线程数量可动态增长
};

// 线程类型
class Thread
{
public:
	// 线程函数类型
	using ThreadFunc = std::function<void(int)>;

	// 线程构造
	Thread(ThreadFunc func)
		: func_(func)
		, threadId_(generateId_++) { }

	// 线程析构
	~Thread() = default;

	// 启动线程
	void start()
	{
		// 创建一个线程来执行一个线程函数
		std::thread t(func_, threadId_);// C++11 有线程对象 t 与线程函数 func_
		t.detach();// 设置线程分离，将线程与对象 t 进行分离，否则当 t 的生命周期结束后，线程也会结束
	}

	// 获取线程 id
	int getId() const
	{
		return threadId_;
	}
private:
	ThreadFunc func_;
	static int generateId_;
	int threadId_;// 保存线程 id
};

int Thread::generateId_ = 0;

// 线程池类型
class ThreadPool
{
public:
	// 线程池构造
	ThreadPool() 
		: initThreadSize_(0)
		, taskSize_(0)
		, idleThreadSize_(0)
		, curThreadSize_(0)
		, taskQueMaxThreshHold_(TASK_MAX_THRESHHOLD)
		, threadSizeThreshHold_(THREAD_MAX_THRESHHOLD)
		, poolMode_(PoolMode::MODE_FIXED)
		, isPoolRunning_(false) { }

	// 线程池析构
	~ThreadPool()
	{
		isPoolRunning_ = false;

		//notEmpty_.notify_all();// 通知正在等待任务而阻塞的线程

		// 等待线程池里面所有的线程返回，线程有两种状态：1.阻塞 & 2.正在执行任务中
		std::unique_lock<std::mutex> lock(taskQueMtx_);
		notEmpty_.notify_all();// 通知正在等待任务而阻塞的线程，先获取锁再唤醒，防止死锁
		exitCond_.wait(lock, [&]()->bool {return threads_.size() == 0; });
	}

	// 设置线程池的工作模型
	void setMode(PoolMode mode)
	{
		if (checkRunningState())
		{
			return;
		}
		poolMode_ = mode;
	}

	// 设置 task 任务队列上线阈值
	void setTaskQueMaxThreshHold(int threshhold)
	{
		if (checkRunningState())
		{
			return;
		}
		taskQueMaxThreshHold_ = threshhold;
	}

	// 设置线程池 cashed 模型下线程阈值
	void setThreadSizeThreshHold(int threshhold)
	{
		if (checkRunningState())
		{
			return;
		}
		if (poolMode_ == PoolMode::MODE_CACHED)
		{
			threadSizeThreshHold_ = threshhold;
		}
	}

	// 给线程池提交任务
	// 使用可变参模板编程，让 submitTask 可以接收任意任务函数和任意数量的参数
	// pool.submitTask(sum1, 10, 20);
	// 函数返回值需要 future<返回值类型>，通过 decltype(func(args...)) 推导出函数返回值类型来实例化 future
	template<typename Func, typename... Args>
	auto submitTask(Func&& func, Args&&... args) -> std::future<decltype(func(args...))>
	{
		// 打包任务，放入任务队列里面
		using RType = decltype(func(args...));// 函数 func 返回值

		// 创建了 packaged_task 函数对象，打包了返回值是 RType，不带参数的函数对象
		// 用 bind 将参数绑定到函数对象 func，所以 packaged_task<RType()>> 可以不带参数
		auto task = std::make_shared<std::packaged_task<RType()>>(
			std::bind(std::forward<Func>(func), std::forward<Args>(args)...));
		std::c<RType> result = task->get_future();// 获取任务的返回值

		std::unique_lock<std::mutex> lock(taskQueMtx_);// 获取锁

		// 线程的通信，等待任务队列有空余，且用户提交任务，最长不能阻塞超过 1s，否则判断提交任务失败返回
		if (!notFull_.wait_for(lock, std::chrono::seconds(1),
			[&]()->bool
			{
				return tasksQue_.size() < (size_t)taskQueMaxThreshHold_;
			}))
		{
			// 表示 notFull_ 等待 1s，条件依然没有满足
			std::cerr << "task queue is full, submit task fail," << std::endl;
			
			// 任务提交失败返回所要任务返回值零值，更准确来说是返回 RType 的默认值，int 类型为 0，double 类型为 0.0
			auto task = std::make_shared<std::packaged_task<RType()>>(
				[]()->RType
				{
					return RType();
				});
			(*task)();// get_future 需执行 task 后才能得到返回值
			return task->get_future();
		}

		// 如果有空余，把任务放入任务队列中
		tasksQue_.emplace([task]()->void
			{
				(*task)();// 封装：解引用执行 task 任务
			});
		taskSize_++;

		notEmpty_.notify_all();// 在 notEmpty 上进行通知

		// 需要根据任务数量和空闲线程的数量，判断是否需要创建新的线程
		// cashed 模式：任务处理比较紧急，场景：小而快的任务
		if (poolMode_ == PoolMode::MODE_CACHED
			&& taskSize_ > idleThreadSize_
			&& curThreadSize_ < threadSizeThreshHold_)
		{
			std::cout << "create new thread..." << std::endl;

			// 创建新的线程对象
			auto ptr = std::make_unique<Thread>(std::bind(&ThreadPool::threadFunc, this, std::placeholders::_1));
			// threads_.emplace_back(std::move(ptr));// std::vector<std::unique_ptr<Thread>>
			int threadId = ptr->getId();
			threads_.emplace(threadId, std::move(ptr));
			threads_[threadId]->start();// 启动线程
			// 修改线程个数相关的变量
			curThreadSize_++;
			idleThreadSize_++;
		}

		// 返回任务的 Result 对象
		return result;
	}

	// 开启线程池
	void start(int initThreadSize = std::thread::hardware_concurrency())// hardware_concurrency 返回系统 CPU 核心数量
	{
		isPoolRunning_ = true;// 设置线程池的启动状态

		initThreadSize_ = initThreadSize;// 记录初始线程个数
		curThreadSize_ = initThreadSize;// 记录当前线程池个数

		// 创建线程对象
		for (int i = 0; i < initThreadSize_; i++)
		{
			// 创建 thread 线程对象时，把线程函数给到 thread 线程对象
			// 在 Thread 里面直接创建线程函数，访问不到 ThreadPool 的成员
			// 所以要由 ThreadPool 把自己的成员函数和自己的对象地址 this 绑定成一个可调用对象，再传给 Thread 执行
			auto ptr = std::make_unique<Thread>(std::bind(&ThreadPool::threadFunc, this, std::placeholders::_1));
			// threads_.emplace_back(std::move(ptr));// this:ThreadPool*，unique_ptr 只有右值拷贝构造，需使用 move，std::vector<std::unique_ptr<Thread>>
			int threadId = ptr->getId();
			threads_.emplace(threadId, std::move(ptr));
		}

		// 启动所有线程
		// 同时启动，保证线程的公平性
		for (int i = 0; i < initThreadSize_; i++)
		{
			threads_[i]->start();// 需要去执行一个线程函数
			idleThreadSize_++;// 记录初始空闲线程的数量
		}
	}

	ThreadPool(const ThreadPool&) = delete;
	ThreadPool& operator=(const ThreadPool&) = delete;
private:
	// 定义线程函数
	void threadFunc(int threadid)
	{
		/*std::cout << "begin threadFunc tid:" << std::this_thread::get_id()
		<< std::endl;

	std::cout << "end threadFunc tid:" << std::this_thread::get_id()
		<< std::endl;*/
		auto lasTime = std::chrono::high_resolution_clock().now();// 记录线程开始时间

		// 所有任务必须执行完成，线程池才可以回收所有线程资源
		for (;;)
		{
			Task task;
			{
				std::unique_lock<std::mutex> lock(taskQueMtx_);// 获取锁

				std::cout << "tid:" << std::this_thread::get_id()
					<< "尝试获取任务..." << std::endl;

				// 区分超时返回还是有任务待执行返回
				// 锁 + 双重判断，防止死锁
				while (tasksQue_.size() == 0)
				{
					if (!isPoolRunning_)
					{
						// 线程池要结束，回收线程资源
						threads_.erase(threadid);
						std::cout << "threadid:" << std::this_thread::get_id() << " exit! "
							<< std::endl;

						exitCond_.notify_all();// 通知，否则析构处会一直阻塞
						return;
					}

					// cached 模式下，回收空闲超过 60s 的线程（超过 initThreadSize_ 的数量）
					if (poolMode_ == PoolMode::MODE_CACHED)
					{
						// 条件变量超时返回了
						if (std::cv_status::timeout ==
							notEmpty_.wait_for(lock, std::chrono::seconds(1)))
						{
							auto now = std::chrono::high_resolution_clock().now();
							auto dur = std::chrono::duration_cast<std::chrono::seconds>(now - lasTime);
							if (dur.count() >= THREAD_MAX_IDLE_TIME
								&& curThreadSize_ > initThreadSize_)
							{
								// 开始回收线程
								// 记录线程数量的相关变量的值修改，把线程对象从线程列表容器中删除，threadid -> thread 对象 -> 删除
								curThreadSize_--;
								idleThreadSize_--;
								threads_.erase(threadid);// 不能传入 std::this_thread::getid()，这是 C++ 库生成的 id 

								std::cout << "threadid:" << std::this_thread::get_id() << " exit! "
									<< std::endl;

								return;
							}
						}
					}
					else
					{
						// 等待 notEmpty 条件
						notEmpty_.wait(lock);
					}
				}

				// 线程池要结束，等待正在阻塞的线程结束，回收线程资源
				if (!isPoolRunning_)
				{
					break;
				}

				idleThreadSize_--;

				std::cout << "tid:" << std::this_thread::get_id()
					<< "获取任务成功！" << std::endl;

				// 从任务队列中取一个任务出来
				task = tasksQue_.front();
				tasksQue_.pop();
				taskSize_--;

				// 如果依然有剩余任务，继续通知其它的线程执行任务
				if (tasksQue_.size() > 0)
				{
					notEmpty_.notify_all();
				}

				notFull_.notify_all();// 取出一个任务，进行通知，通知可以继续提交生产
			}// 释放锁，让这个线程获取到任务就释放锁给其他线程操作

			// 当前线程负责执行这个任务
			if (task != nullptr)
			{
				task();// 执行 function<void()>
			}

			lasTime = std::chrono::high_resolution_clock().now();// 更新线程执行完任务的时间
			idleThreadSize_++;
		}
	}

	// 检查 pool 运行状态
	bool checkRunningState() const
	{
		return isPoolRunning_;
	}
private:
	//std::vector<std::unique_ptr<Thread>> threads_;// 线程列表，每个线程需附带一个 id，方便 cached 模型销毁线程
	std::unordered_map<int, std::unique_ptr<Thread>> threads_;// 线程列表

	int initThreadSize_;// 初始的线程数量
	int threadSizeThreshHold_;// 线程数量上限阈值
	std::atomic_int curThreadSize_;// 记录当前线程池里面线程的总数量
	std::atomic_int idleThreadSize_;// 记录空闲线程的数量

	// Task 任务 -> 函数对象
	using Task = std::function<void()>;
	std::queue<Task> tasksQue_;// 任务队列
	std::atomic_int taskSize_;// 任务数量，用原子类型保证线程安全
	int taskQueMaxThreshHold_;// 任务队列数量上限阈值

	std::mutex taskQueMtx_;// 保证任务队列的线程安全
	std::condition_variable notFull_;// 表示任务队列不满
	std::condition_variable notEmpty_;// 表示任务队列不空
	std::condition_variable exitCond_;// 等待线程资源全部回收

	PoolMode poolMode_;// 当前线程池的工作模式
	std::atomic_bool isPoolRunning_;// 表示当前线程池的启动状态
};

#endif