# © Copyright Databand.ai, an IBM Company 2022

import random
import threading

from dbnd._core.utils.basics import memoized


r = random.Random(x=1)


def _get_rand():
    return r.randint(0, 100000)


class TestMemoized(object):
    def test_basics(self):
        @memoized.cached()
        def a(some_p=2):
            return _get_rand()

        @memoized.cached()
        def b():
            return _get_rand()

        assert a() == a()  # cached!
        assert b() == b()  # cached!
        assert a() != b()  # not same cache!

    def test_threadsafe(self):
        @memoized.per_thread_cached()
        def a(some_p=22):
            return _get_rand()

        @memoized.per_thread_cached()
        def b():
            return _get_rand()

        assert a() == a()  # cached
        assert _call_thread(a) != a()  # not same
        assert _call_thread(b) != b()  # not same thread
        assert a() != b()  # not same cache!


def _call_thread(f):
    result = []

    def _run():
        r = f()
        result.append(r)

    thread1 = threading.Thread(target=_run, args=())
    thread1.start()
    thread1.join()
    return result[0]
