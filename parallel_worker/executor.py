from multiprocessing import Manager, Process, Queue
import threading
import logging 
from parallel_worker.interrupt import init_shutdown_event, init_signal
import time 
import queue
from tqdm import tqdm

def init_main_logger():
    logger = logging.getLogger(f'main')
    logger.setLevel(logging.DEBUG)
    fh = logging.FileHandler(f'main.log')
    fh.setLevel(logging.DEBUG)
    logger.addHandler(fh)
    return logger 

def run_logger_thread(queue):
    main_logger = logging.getLogger('main')
    main_logger.debug('start logger thread')
    while True:
        record = queue.get()
        if record is None:
            break
        main_logger.debug(record)

def get_job_logger(process_index):
    logger = logging.getLogger(f'{process_index}')
    logger.setLevel(logging.DEBUG)
    fh = logging.FileHandler(f'{process_index}.log')
    fh.setLevel(logging.DEBUG)
    logger.addHandler(fh)
    return logger

def process_outputs(output_queue, output_function, shutdown_event, job_queue):
    while not shutdown_event.is_set():
        try:
            record = output_queue.get(block=True, timeout=0.05)
        except queue.Empty:
            continue  

        if record is None:
            break

        output_function(record)

    if shutdown_event.is_set():
        job_queue.put("SHUTDOWN EVENT SET FOR OUTPUT PROCESSOR")

def fill_job_queue(generator_func, job_queue, n_jobs):
    for item in generator_func():
        job_queue.put(item)

    for _ in range(n_jobs):
        job_queue.put(None)

def process(process_index, logger_queue, output_queue, job_queue, worker_function, shutdown_event):
    try:        
        job_logger = get_job_logger(process_index)
        init_signal(shutdown_event, job_logger)

        while not shutdown_event.is_set():
            try:
                item = job_queue.get(block=True, timeout=0.05)
            except queue.Empty:
                continue        
            
            if item is None:
                break
            
            result = worker_function(job_logger, item)
            output_queue.put(result)
        
        if shutdown_event.is_set():
            job_logger.debug("SHUTDOWN EVENT SET")
        else:
            job_logger.debug("WORK DONE")
    except Exception as e:
        print(e)
        #e = traceback.print_exception(value=e, tb=e.__traceback__, etype=Exception)
        logger_queue.put(f'Exc: {e} in job {process_index}')

class Executor():
    def __init__(self, n_jobs, worker_func, generator_func, output_func):
        m = Manager()
        self.logger_queue = m.Queue()
        self.job_queue = m.Queue()
        self.output_queue = m.Queue()
        self.jobs = []
        self.n_jobs = n_jobs
        self.logger = init_main_logger()
        self.shutdown_event = init_shutdown_event()
        self.worker_func = worker_func
        self.generator_func = generator_func
        self.output_func = output_func
        self.STOP_WAIT_SECS = 2
        init_signal(self.shutdown_event, self.logger)

    def start_logger_thread(self):
        self.lp = threading.Thread(target=run_logger_thread, args=(self.logger_queue,))
        self.lp.start()

    def start_output_writer(self):
        self.logger.debug(f'Start output process')
        self.op = Process(target=process_outputs, args=(self.output_queue, self.output_func, self.shutdown_event, self.job_queue))
        self.op.start()

    def start_fill_job_queue(self):
        # must be multiprocessed too, otherwise waiting for process afterwards dont work
        self.logger.debug(f'Start generating work')
        self.generator_process = Process(target=fill_job_queue, args=(self.generator_func, self.job_queue, self.n_jobs))
        self.generator_process.start()

    def start(self):
        self.start_logger_thread()
        self.start_output_writer()

        # start worker jobs
        for i in range(self.n_jobs):
            self.logger.debug(f'Start worker process {i}')
            job = Process(target=process, args=(i, self.logger_queue, self.output_queue, self.job_queue, self.worker_func, self.shutdown_event))
            job.daemon = True  
            job.start()
            self.jobs.append(job)

        self.start_fill_job_queue()

        # Wait for processes
        self.wait_for_generation()
        self.wait_for_processing()
        self.wait_for_output_processing()
        self.wait_for_logger()

    def wait_for_generation(self):
        self.logger.debug('wait for generation')
        self.generator_process.join()

    def wait_for_processing(self):
        self.logger.debug('wait for processing')
        for worker_process in self.jobs:
            print(worker_process)
            worker_process.join()

    def wait_for_output_processing(self):
        self.logger.debug('wait for output processing')
        self.output_queue.put(None)
        self.op.join()
    
    def wait_for_logger(self):
        self.logger.debug('wait for logger')
        self.logger_queue.put(None)
        self.lp.join()

    def stop(self):
        # start() blocks - not needed

        self.shutdown_event.set()

        end_time = time.time() + self.STOP_WAIT_SECS
        num_terminated = 0
        num_failed = 0

        # -- Wait up to STOP_WAIT_SECS for all processes to complete
        for proc in self.jobs:
            join_secs = max(0.0, min(end_time - time.time(), self.STOP_WAIT_SECS))
            proc.join(join_secs)

        # -- Clear the procs list and _terminate_ any procs that
        # have not yet exited
        while self.jobs:
            proc = self.jobs.pop()
            if proc.is_alive():
                proc.terminate()
                num_terminated += 1
            else:
                exitcode = proc.exitcode
                if exitcode:
                    num_failed += 1

        return num_failed, num_terminated


