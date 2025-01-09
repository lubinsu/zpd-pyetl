import sched
import time
import threading

# 创建一个调度器实例
scheduler = sched.scheduler(time.time, time.sleep)


def run_timed_task():
    # 调度下一个任务
    task_thread = threading.Thread(target=timed_task)
    task_thread.start()

    print("定时任务启动了！", time.strftime("%Y-%m-%d %H:%M:%S"))

    schedule_next()


def timed_task():
    print("定时任务执行了！", time.strftime("%Y-%m-%d %H:%M:%S"))
    time.sleep(8)
    print("定时任务结束了！", time.strftime("%Y-%m-%d %H:%M:%S"))
    print("**************************************************")


def schedule_next():
    # 每分钟调度一次
    scheduler.enter(5, 1, run_timed_task)  # 60秒后执行


def run_scheduler():
    # 启动第一个任务
    now = time.time()
    next_minute = now + 60 - (now % 60)
    scheduler.enterabs(next_minute, 1, run_timed_task)  # 在第一个整数分钟执行
    # 启动调度器
    scheduler.run()


# 创建并启动调度器线程
thread = threading.Thread(target=run_scheduler)
thread.start()

print("调度器线程已启动，将每分钟执行一次定时任务。")
