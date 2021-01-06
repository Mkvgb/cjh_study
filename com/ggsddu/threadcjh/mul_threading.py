import threading
import time


def run(n):
    print("task  ", n)
    print(type(threading.currentThread().ident))
    time.sleep(2)


start_time = time.time()
t1 = threading.Thread(target=run, args=("t1",))
t2 = threading.Thread(target=run, args=("t2",))

t1.start()
t2.start()
t1.join()
t2.join()
print(time.time() - start_time)
