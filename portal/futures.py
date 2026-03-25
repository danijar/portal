import threading


class Future:
    def __init__(self):
        self.rai = [False]
        self.fns = []
        self.con = threading.Condition()
        self.don = False
        self.res = None
        self.err = None

    def __repr__(self):
        if not self.don:
            info = 'done=False'
        elif self.err:
            info = f"done=True, error='{self.err}' raised={self.rai[0]}"
        else:
            info = 'done=True'
        return f'Future({info})'

    def wait(self, timeout=None):
        if self.don:
            return self.don
        with self.con:
            return self.con.wait(timeout)

    def done(self):
        return self.don

    def error(self):
        return self.err

    def result(self, timeout=None):
        if not self.wait(timeout):
            raise TimeoutError
        assert self.don
        if self.err is None:
            return self.res
        if not self.rai[0]:
            self.rai[0] = True
            raise self.err

    def set_result(self, result):
        with self.con:
            assert not self.don
            self.don = True
            self.res = result
            for fn in self.fns:
                fn(self)
            self.con.notify_all()

    def set_error(self, e):
        with self.con:
            assert not self.don
            self.don = True
            self.err = e
            for fn in self.fns:
                fn(self)
            self.con.notify_all()

    def add_callback(self, fn):
        with self.con:
            self.fns.append(fn)
            if self.don:
                fn(self)

    def remove_callback(self, fn):
        self.fns.remove(fn)


def wait(futures, timeout=None, amount=None):
    if amount is None:
        amount = len(futures)
    waiter = _Waiter(amount)
    for future in futures:
        future.add_callback(waiter)
    result = waiter.wait(timeout)
    for future in futures:
        future.remove_callback(waiter)
    if result:
        return tuple(waiter.completed)
    else:
        return False


class _Waiter:
    def __init__(self, amount):
        self.remaining = amount
        self.event = threading.Event()
        self.lock = threading.Lock()
        self.completed = []

    def wait(self, timeout=None):
        return self.event.wait(timeout)

    def __call__(self, future):
        self.completed.append(future)
        if self.remaining <= 0:
            return
        with self.lock:
            self.remaining -= 1
            if self.remaining <= 0:
                self.event.set()
