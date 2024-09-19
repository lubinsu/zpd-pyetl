import fcntl
import sys
import threading
from datetime import *
import time
import inspect
import ctypes
from signal import pthread_kill, SIGKILL, SIGABRT
from multiprocessing import Process


def flock_task(file):
    try:
        with open(file, "w") as f:
            # 获取文件锁
            # fcntl.flock(f.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            print("开始获取文件锁")
            fcntl.flock(f.fileno(), fcntl.LOCK_EX)
            print("文件锁获取成功")
            time.sleep(10)
            # 在文件中写入数据
            f.write("hello world\n")
            print("文件锁释放成功")
            # 释放文件锁
            fcntl.flock(f.fileno(), fcntl.LOCK_UN)
    except BlockingIOError as e:
        print("文件已被锁定")
    except Exception as e:
        with open(file, "w") as f:
            fcntl.flock(f.fileno(), fcntl.LOCK_UN)


if __name__ == '__main__':
    timeout = int(sys.argv[1])
    print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    threads = []

    t1 = Process(target=flock_task, name="py_crontab_1", args=("/tmp/py_crontab_1.lock",))
    t2 = Process(target=flock_task, name="py_crontab_2", args=("/tmp/py_crontab_2.lock",))

    threads.append(t1)
    threads.append(t2)

    for t in threads:
        t.start()

    for t in threads:
        if t.name == "py_crontab_1":
            t.join(timeout=timeout)  # 等待所有线程完成
            if t.is_alive():  # 如果线程仍然在运行，则终止它
                print("线程超时，强制终止线程")
                t.terminate()  # 强制停止线程（注意：这不是推荐的做法，因为它可能会导致资源泄露或其他问题）
                print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            else:
                print("线程正常结束")
        else:
            t.join()

    print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
