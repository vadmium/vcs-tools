import sys
import weakref
from os.path import basename

class Function(object):
    def __init__(self):
        self.__name__ = type(self).__name__
    def __get__(self, obj, cls):
        if obj is None:
            return self
        return Binding(self, obj)
class Binding:
    def __init__(self, func, obj):
        self.func = func
        self.obj = obj
    def __call__(self, *args, **kw):
        return self.func(self.obj, *args, **kw)

class exc_sink(Function):
    """Decorator wrapper to trap all exceptions raised from a function to the
    default exception hook"""
    def __init__(self, inner):
        self.inner = inner
    def __call__(self, *args, **kw):
        try:
            return self.inner(*args, **kw)
        except BaseException as e:
            sys.excepthook(type(e), e, e.__traceback__)

class weakmethod(object):
    """Decorator wrapper for methods that binds to objects using a weak
    reference"""
    def __init__(self, func):
        self.func = func
    def __get__(self, obj, cls):
        if obj is None:
            return self
        return WeakBinding(self.func, obj)
class WeakBinding(Function):
    def __init__(self, func, obj):
        self.func = func
        self.ref = weakref.ref(obj)
    def __call__(self, *args, **kw):
        obj = self.ref()
        if obj is None:
            raise ReferenceError("dead weakly-bound method {} called".
                format(self.func))
        return self.func.__get__(obj, type(obj))(*args, **kw)
    def __repr__(self):
        return "<{} of {} to {}>".format(
            type(self).__name__, self.func, self.ref())

def gen_repr(gi):
    f = gi.gi_frame
    if f:
        return "<{} {:#x}, {}:{}>".format(f.f_code.co_name, id(gi),
            basename(f.f_code.co_filename), f.f_lineno)
    else:
        return "<{} {:#x} (inactive)>".format(gi.gi_code.co_name,
            id(gi))
