from parallel_worker.executor import Executor


def work(logger, item):
    import time 
    time.sleep(1)
    logger.debug(item)
    #raise Exception("bla")
    return item

def gen():
    for i in range(20):
        yield str(i)

def output(record):
    print(record)
    with open("test.txt", "a") as f:
        f.write(record)

e = Executor(2, work, gen, output)
e.start()
#e.stop()