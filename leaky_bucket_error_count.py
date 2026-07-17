#!/usr/bin/env python3

import threading
from collections import defaultdict, deque
import time
import logging
import os
import re
import subprocess
import sys


"""
references:
 - http://blog.ianbicking.org/good-catch-all-exceptions.html
"""


class LeakingErrorCounter:
    """A leaky bucket type of exception counter

    @param decay_rate decay rate of errors in Hz, e.g. 2 means after 0.5
        seconds the error count decayed to zero
    @param error_limit the error limit after which the exception should not be
        catched and ignored but re-raised
    @param ignore a list of exceptions which should not be catched but
        immediately re-raised, i.e. 'MemoryError' or something like that
    @param decay_in_background whether to run the time-based decay in a
        background thread (default). Set to False to disable automatic decay,
        e.g. when occurrences should be counted per event instead of per time.

    Instantiate the error counter and call the 'handle_exception' method on
    each exception or just the ones that should not immediately be re-raised.
    After the same type of exception was encountered more than 'error_limit'
    times in a short time interval, i.e. before the error ocurrences have
    decayed away, the exception is re-raised.

    This class should be especially useful as a 'last resort' catcher if
    continuity is preferred over integrity of the running program. An example
    would be if the user of the program is not interested in program bugs but
    it works Good Enough (tm) in most cases.
    """
    def __init__(self, decay_rate=2, error_limit=10, ignore=None,
                 decay_in_background=True):
        self.errorcnt = defaultdict(int)
        self.decay_rate = decay_rate
        self.error_limit = error_limit
        self.ignore = tuple(ignore) if ignore else ()
        self._lock = threading.Lock()
        self.decay_thread = None
        if decay_in_background:
            self.decay_thread = threading.Thread(target=self.run, daemon=True)
            self.decay_thread.start()

    def run(self):
        while True:
            time.sleep(1.0 / self.decay_rate)
            self.decay()

    def decay(self, decrement=1):
        with self._lock:
            for k in list(self.errorcnt.keys()):
                if self.errorcnt[k] > 0:
                    self.errorcnt[k] -= decrement

    def handle_exception(self, e):
        if self.ignore and isinstance(e, self.ignore):
            raise e
        k = str(e)
        with self._lock:
            self.errorcnt[k] += 1
            count = self.errorcnt[k]
        logging.debug("exception: %s, errorcount: %d", k, count)
        if self.error_limit ==0:
            logging.info("Exception %s encountered, but error limit not set so keep running", e)
        elif count > self.error_limit:
            logging.error("error limit hit for exception %s, reraising", k)
            raise e
        else:
            logging.info("Exception %s encountered, error count increased", e)


def continous_run_with_leaky_error_counter(fun, instance=None, run_condition=lambda: True):
    if instance is None:
        instance = LeakingErrorCounter()
    while run_condition():
        try:
            fun()
        except Exception as e:
            instance.handle_exception(e)


def test_fails_after_too_many_errors_in_too_short_time():
    """This test throws one of two errors until too many have been encountered of one type"""
    import random
    yield_list = [Exception("generic error"), Exception("other error")]

    def error_thrower():
        time.sleep(0.1)
        logging.debug("throwing_error")
        raise random.choice(yield_list)
    continous_run_with_leaky_error_counter(error_thrower)


_EXCEPTION_RE = re.compile(
    r'^[A-Za-z_][A-Za-z0-9_.]*'
    r'(Error|Exception|Interrupt|Timeout|Exit|Warning)([: ].*)?$'
)


def error_signature(returncode, tail):
    """Build a stable failure signature from an exit code and output tail.

    Prefer the last Python-exception-looking line, else the last non-empty
    line; normalize away volatile tokens (hex addresses, digits, whitespace)
    so the same underlying failure maps to the same signature.
    """
    sig = ""
    for line in tail:
        if _EXCEPTION_RE.match(line):
            sig = line
    if not sig:
        for line in tail:
            if line.strip():
                sig = line
    sig = re.sub(r'0x[0-9a-fA-F]+', 'H', sig)
    sig = re.sub(r'\d+', 'N', sig)
    sig = re.sub(r'\s+', ' ', sig).strip()
    return "%d|%s" % (returncode, sig)


def _run_once(cmd, tail_lines=100):
    """Run cmd, stream combined output to stdout, return (returncode, tail)."""
    tail = deque(maxlen=tail_lines)
    proc = subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
        bufsize=1,
    )
    for line in proc.stdout:
        sys.stdout.write(line)
        sys.stdout.flush()
        tail.append(line.rstrip("\n"))
    proc.wait()
    return proc.returncode, list(tail)


def retry_main(argv):
    """Run a command with leaky-bucket retries, giving up early on repeats.

    Usage: leaky_bucket_error_count.py -- <command> [args...]
    Env:
        MAX_RETRIES       absolute attempt cap (default 7)
        BACKOFF_FACTOR    sleep BACKOFF_FACTOR*attempt seconds between tries
                          (default 60)
        SAME_ERROR_LIMIT  give up after the same error recurs this many times;
                          0 disables early give-up (default 2)
    """
    args = argv[1:]
    if args and args[0] == "--":
        args = args[1:]
    if not args:
        print("usage: %s -- <command> [args...]" % argv[0], file=sys.stderr)
        return 2

    max_retries = int(os.environ.get("MAX_RETRIES", "7"))
    backoff = int(os.environ.get("BACKOFF_FACTOR", "60"))
    error_limit = int(os.environ.get("SAME_ERROR_LIMIT", "2"))
    counter = LeakingErrorCounter(
        error_limit=error_limit, decay_in_background=False,
    )

    attempt = 1
    returncode = 1
    while True:
        returncode, tail = _run_once(args)
        if returncode == 0:
            return 0
        signature = error_signature(returncode, tail)
        try:
            counter.handle_exception(Exception(signature))
        except Exception:
            logging.error(
                "leaky-bucket: error %r recurred %d times, giving up early",
                signature, error_limit,
            )
            return returncode
        if attempt >= max_retries:
            logging.error(
                "leaky-bucket: reached MAX_RETRIES=%d, giving up", max_retries,
            )
            return returncode
        delay = backoff * attempt
        logging.warning(
            "leaky-bucket: attempt %d/%d failed (rc=%d), retrying in %ds",
            attempt, max_retries, returncode, delay,
        )
        time.sleep(delay)
        attempt += 1


if __name__ == "__main__":
    if len(sys.argv) > 1:
        logging.basicConfig(level=logging.INFO, format="%(message)s")
        sys.exit(retry_main(sys.argv))
    logging.root.setLevel(logging.DEBUG)
    test_fails_after_too_many_errors_in_too_short_time()
    logging.debug("program exited successful")
