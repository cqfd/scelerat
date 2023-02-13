from celery import Celery
from celery.canvas import Signature
from typing import Optional

celery = Celery(broker="amqp://guest:guest@rabbit//", backend="redis://redis")

@celery.task(queue="continuation", name="tasks.do_stuff_with_response", bind=True)
def do_stuff_with_response(self, response: str, name: str) -> None:
    print(f"Hi {name}!")
    print(f"Who am I? {locals()=}")
    print(response)