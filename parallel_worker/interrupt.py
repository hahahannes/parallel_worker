import functools
import signal 
from multiprocessing import Event

class SignalObject:
    MAX_TERMINATE_CALLED = 3

    def __init__(self, shutdown_event):
        self.terminate_called = 0
        self.shutdown_event = shutdown_event

def default_signal_handler(
        signal_object,
        exception_class,
        logger,
        signal_num,
        current_stack_frame):
    signal_object.terminate_called += 1
    logger.debug(f"SUTDOWN SIGNAL {signal_num} CALLED {signal_object.terminate_called}")
    signal_object.shutdown_event.set()
    if signal_object.terminate_called == signal_object.MAX_TERMINATE_CALLED:
        raise exception_class()

def init_signal(shutdown_event, logger):
    signal_object = SignalObject(shutdown_event)
    signal_num = signal.SIGINT
    handler = functools.partial(default_signal_handler, signal_object, KeyboardInterrupt, logger)
    signal.signal(signal_num, handler)
    signal.siginterrupt(signal_num, False)

def init_shutdown_event():
    return Event()