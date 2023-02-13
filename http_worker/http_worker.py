from gevent import monkey
monkey.patch_all()

from celery import Celery
from celery.canvas import Signature

celery = Celery(
    broker="amqp://guest:guest@rabbit//",
    backend="redis://redis",
    result_extended=True,
    task_remote_tracebacks=True,
)

@celery.task(
    queue="http",
    name="lightweight_http.request",
    retry_backoff=2,
    retry_jitter=True,
    bind=True
)
def request(self, url: str) -> str:
    print(f"Who am I? {locals()=}")
    import requests
    return requests.get(url).text


# I'm curious how error links work if the celery process that produced the error
# doesn't know how to invoke the linked task in-process.
@celery.task(name="lightweight_http.boom")
def boom(arg: int) -> None:
    print("Ok, going to go boom!", arg)
    raise AssertionError(f"Boom! {arg=}")