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

// Any 类型，表示可以接收任意数据类型，此类型在 C++17 中可以直接调用
class Any
{
public:
	// 为了规范与减少可能的编译错误，给出默认构造与析构
	Any() = default;
	~Any() = default;

	// std::unique_ptr<Base> base_不接收左值，所以需禁止左值的拷贝构造
	Any(const Any&) = delete;
	Any& operator=(const Any&) = delete;

	// 为了规范与减少可能的编译错误，给出右值默认构造与析构
	Any(Any&&) = default;
	Any& operator=(Any&&) = default;

	// 这个构造函数可以让 Any 类型接收其他任意其他的数据
	template<typename T>
	Any(T data)
		: base_(std::make_unique<Derive<T>>(data)) { }

	// 这个方法能把 Any 对象里面存储的 data 数据提取出来
	template<typename T>
	T cast()
	{
		Derive<T>* pd = dynamic_cast<Derive<T>*>(base_.get());// 基类指针 -> 派生类指针 RTTI
		if (pd == nullptr)
		{
			throw "type is unmatched!";
		}

		return pd->data_;
	}
private:
	// 基类类型
	class Base
	{
	public:
		// irtual ~Base() {};
		virtual ~Base() = default;// 默认实现，编译器会做额外优化，使用这种方式更好
	};

	// 派生类类型
	template<typename T>
	class Derive : public Base
	{
	public:
		Derive(T data)
			:data_(data) { }
		T data_;// 保存了任意其他类型
	};
private:
	std::unique_ptr<Base> base_;// 定义一个基类指针
};

// 信号量类
class Semaphore
{
public:
	Semaphore(int limit = 0) : resLimit(limit) { }
	// Linux 下实现
	//~Semaphore()
	//{
	//	isExit_ = true;
	//}

	~Semaphore() = default;

	// 获取一个信号量资源
	void wait()
	{
		// Linux 下添加
		//if (isExit_ == true)
		//{
		//	return;
		//}

		std::unique_lock<std::mutex> lock(mtx_);
		// 等待信号量有资源，没有资源会阻塞当前线程
		cond_.wait(lock,
			[&]()->bool
			{
				return resLimit > 0;
			});
		resLimit--;
	}

	// 增加一个信号量资源
	void post()
	{
		// Linux 下添加
		//if (isExit_ == true)
		//{
		//	return;
		//}

		std::unique_lock<std::mutex> lock(mtx_);
		resLimit++;
		// Linux 下条件变量 condition_variable 的析构不会释放相应的资源
		// 真正的根因是 Result 生命周期结束太早，导致工作线程访问了已经析构的对象
		// 导致这里状态失效，无故阻塞
		cond_.notify_all();
	}
private:
	//std::atomic_bool isExit_;// Linux 下实现
	int resLimit;
	std::mutex mtx_;
	std::condition_variable cond_;
};

// 实现接收任务提交到线程池的 task 任务执行完后的返回值类型 Result
class Task;
class Result
{
public:
	Result(std::shared_ptr<Task> task, bool isValid = true);
	~Result() = default;

	// 获取任务执行完后的返回值
	void setVal(Any any);

	// 用户调用这个方法获取 task 的返回值
	Any get();
private:
	Any any_;// 存储任务的返回值
	Semaphore sem_;// 线程通信信号量
	std::shared_ptr<Task> task_;// 指向对应获取返回值的任务对象
	std::atomic_bool isValid_;// 返回值是否有效
};

// 任务抽象基类
// 用户可以自定义任意任务类型，从 Task 继承， 重写 run 方法，实现自定义任务处理
class Task
{
public:
	Task();
	~Task() = default;
	// 封装 run
	void exec();
	void setResult(Result* res);

	virtual Any run() = 0;
private:
	Result* result_;// 不能使用强智能指针，会导致循环引用问题，此处 Result 对象的声明周期 > Task 对象的声明周期，可以不使用强智能指针
};

// 线程池支持的模式
// 添加 class PoolMode ，防止因枚举类型不同，但是枚举项相同，造成枚举名字冲突
enum class PoolMode
{
	MODE_FIXED,// 固定数量的线程
	MODE_CACHED,// 线程数量可动态增长
};

/*
example:
ThreadPool pool;
pool.start(4);

class MyTask : public Task
{
public:
	void run() { //线程代码... }
};

pool.submitTask(std::shared_ptr<MyTask>());

*/

// 线程类型
class Thread
{
public:
	// 线程函数类型
	using ThreadFunc = std::function<void(int)>;

	// 线程构造
	Thread(ThreadFunc func);

	// 线程析构
	~Thread();

	// 启动线程
	void start();

	// 获取线程 id
	int getId() const;
private:
	ThreadFunc func_;
	static int generateId_;
	int threadId_;// 保存线程 id
};

// 线程池类型
class ThreadPool
{
public:
	// 线程池构造
	ThreadPool();
	
	// 线程池析构
	~ThreadPool();

	// 设置线程池的工作模型
	void setMode(PoolMode mode);

	// 设置 task 任务队列上线阈值
	void setTaskQueMaxThreshHold(int threshhold);

	// 设置线程池 cashed 模型下线程阈值
	void setThreadSizeThreshHold(int threshhold);

	// 给线程池提交任务
	Result submitTask(std::shared_ptr<Task> sp);

	// 开启线程池
	void start(int initThreadSize = std::thread::hardware_concurrency());// hardware_concurrency 返回系统 CPU 核心数量

	ThreadPool(const ThreadPool&) = delete;
	ThreadPool& operator=(const ThreadPool&) = delete;
private:
	// 定义线程函数
	void threadFunc(int threadid);

	// 检查 pool 运行状态
	bool checkRunningState() const;
private:
	//std::vector<std::unique_ptr<Thread>> threads_;// 线程列表，每个线程需附带一个 id，方便 cached 模型销毁线程
	std::unordered_map<int, std::unique_ptr<Thread>> threads_;// 线程列表

	int initThreadSize_;// 初始的线程数量
	int threadSizeThreshHold_;// 线程数量上限阈值
	std::atomic_int curThreadSize_;// 记录当前线程池里面线程的总数量
	std::atomic_int idleThreadSize_;// 记录空闲线程的数量
	
	// 若传入了临时对象，出作用域会自动析构，此时用裸指针会指向一个已经析构的对象，无法访问任何内容，此时应采用智能指针延长对象的生命周期
	// std::queue<Task*> tasks_;// 任务列表
	std::queue<std::shared_ptr<Task>> tasksQue_;// 任务队列
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

