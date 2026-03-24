import threading


class Future:
    def __init__(self, waitfn=None):
        self.rai = [False]
        self.waitfn = waitfn
        self.waiters = set()
        self.con = threading.Condition()
        self.don = False
        self.res = None
        self.err = None

    def __repr__(self):
        if not self.done:
            info = 'done=False'
        elif self.err:
            info = f"done=True', error='{self.err}' raised={self.rai[0]}"
        else:
            info = 'done=True'
        return f'Future({info})'

    def wait(self, timeout=None):
        if self.don:
            return self.don
        if self.waitfn:
            self.waitfn(self, timeout)
            return self.done()
        else:
            with self.con:
                return self.con.wait(timeout)

    def done(self):
        return self.don

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
        assert not self.don
        self.don = True
        self.res = result
        for waiter in self.waiters:
            waiter(self, error=False)
        with self.con:
            self.con.notify_all()

    def set_error(self, e):
        assert not self.don
        self.don = True
        self.err = e
        for waiter in self.waiters:
            waiter(self, error=True)
        with self.con:
            self.con.notify_all()


def wait(futures, timeout=None, amount=None):
    if amount is None:
        amount = len(futures)
    waiter = _Waiter(amount)
    for future in futures:
        future.waiters.add(waiter)
    result = waiter.wait(timeout)
    for future in futures:
        future.waiters.remove(waiter)
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

    def __call__(self, future, error):
        del error
        self.completed.append(future)
        if self.remaining <= 0:
            return
        with self.lock:
            self.remaining -= 1
            if self.remaining <= 0:
                self.event.set()
