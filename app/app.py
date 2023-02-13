import requests
import typing
import celery.canvas
from celery import Celery, Task
from celery.canvas import Signature
from fastapi import FastAPI
from typing import ParamSpec, TypeVar, Callable, Generic, Concatenate, Any

app = FastAPI()
celery = Celery("tasks", broker="amqp://guest:guest@rabbit:5672//")
celery.set_default()

P = ParamSpec("P")
T = TypeVar("T", covariant=True)
U = TypeVar("U", covariant=True)

X = TypeVar("X", contravariant=True)

class _task(Generic[P, T]):
    def __init__(self, func: Callable[P, T], **kwargs: Any) -> None:
        self.celery_task = celery.task(**kwargs)(func)

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> T:
        return typing.cast(T, self.celery_task(*args, **kwargs))
    
    def s(self, *args: P.args, **kwargs: P.kwargs) -> Signature:
        return self.celery_task.s(*args, **kwargs).set(queue=self.celery_task.queue)

def task(**kwargs: Any) -> Callable[[Callable[P, T]], _task[P, T]]:
    def wrapper(func: Callable[P, T]) -> _task[P, T]:
        return _task(func, **kwargs)
    
    return wrapper


class continuation(Generic[X, U]):
    def __init__(self, task: _task[Concatenate[X, P], U], *args: P.args, **kwargs: P.kwargs) -> None:
        self.task = task
        self.args = args
        self.kwargs = kwargs
        self._options: dict[str, Any] = dict(queue=task.celery_task.queue)
    
    def with_celery_options(self, **options: Any) -> "continuation[X, U]":
        self._options |= options
        return self
    
    def s(self) -> Signature:
        return self.task.celery_task.s(*self.args, **self.kwargs).set(**self._options)

    
# I'm not really clear on what these kwargs are for/where they "get applied"
@task(queue="continuation", name="tasks.do_stuff_with_response")
def do_stuff_with_response(response: str, name: str) -> None:
    print(f"Hi {name}!")
    print(response)

def async_http_sig(url: str) -> Signature:
    return celery.signature(
        "lightweight_http.request",
        queue="http",
        args=[url],
    )

class AsyncHttpRequest(Generic[P]):
    def __init__(self, url: str, *, with_celery_options: dict[str, Any] | None = None) -> None:
        self.url = url
        self._celery_options = with_celery_options or {}
        self._got_sent = False
        self._and_then: continuation[str, None] | None = None
    
    def and_then(self, k: continuation[str, None]) -> "AsyncHttpRequest[P]":
        assert self._and_then is None
        self._and_then = k
        return self
    
    def apply_async(self) -> None:
        chain = celery.signature("lightweight_http.request", args=[self.url], queue="http", **self._celery_options)
        if self._and_then is not None:
            chain |= self._and_then.s()
        chain.apply_async(eta=1)
        self._got_sent = True
    
    def __del__(self) -> None:
        if not self._got_sent:
            print("Warning: AsyncHttpRequest was constructed but never sent! Did you forget to call .apply_async?")



def thunk(f: Callable[P, T], *args: P.args, **kwargs: P.kwargs) -> Callable[[], T]:
    return lambda: f(*args, **kwargs)

def example(x: int, y: str) -> None:
    print(f"Hi {x}! {y}")

t = thunk(example, 1, "foo")


@app.get("/{name}")
def read_root(name: str) -> dict[str, Any]:
    AsyncHttpRequest(
        "http://httpbin.org/get",
        with_celery_options=dict(countdown=10),
    ).and_then(
        continuation(do_stuff_with_response, name=name).with_celery_options(countdown=10)
    ).apply_async()

    return {"Hello": "World"}