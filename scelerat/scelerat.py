"""
Provides a slick way to coordinate celery tasks by using coroutines.
"""

import types
import typing
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import (
    Any,
    Callable,
    Coroutine,
    Generator,
    Generic,
    Literal,
    ParamSpec,
    TypedDict,
    TypeVar,
    overload,
)

import celery
from celery.canvas import Signature as CelerySignature


# Example workflows


async def parallel_stuff(urls: list[str]) -> dict[str, str | None]:
    try:
        results = await parallel(
            {url: get_url(url) for url in urls} | {"boom": boom(123)}
        )
        return results
    except TaskError as e:
        print("Caught exception:", e)
        return {url: None for url in urls}


async def example(urls: list[str]) -> int:
    # Can do a bunch of tasks one after the other.
    synchronous_results = [
        await get_url(url).with_celery_options(countdown=1) for url in urls
    ]

    import random

    # Remember to memoize effectul computations so that they only happen once!
    n = await side_effect(random.randint, 1, 5)
    print(f"Sleeping for {n} seconds...")
    await sleep(n)

    # Can also do things in parallel.
    parallel_results = await parallel(
        {url: get_url(url).with_celery_options(countdown=1) for url in urls}
    )
    print(parallel_results)

    print("Returning the length of all results combined")
    return sum(
        len(result) for result in synchronous_results + list(parallel_results.values())
    )


async def failure_example(arg: int) -> None:
    try:
        await boom(arg)
    except Exception as e:
        print("Caught exception:", e)
    return None


# Implementation

T = TypeVar("T", covariant=True)

JSON = str | int | float | bool | None | list["JSON"] | dict[str, "JSON"]
J = TypeVar("J", bound=JSON, covariant=True)
Thunk = Callable[[], J]

# A Workflow[T] is a coroutine that yields CelerySignatures or Thunks,
# receives JSON in response, and eventually returns some specific kind of JSON.
Workflow = Coroutine[list[CelerySignature] | Thunk[JSON], JSON, J]


@dataclass
class Task(Generic[J]):
    signature: CelerySignature

    # Not sure why I have to use Any here 🤔 But mypy doesn't like it if I use
    # J instead.
    def __await__(self) -> Generator[list[CelerySignature], Any, J]:
        [result] = yield [self.signature]
        return typing.cast(J, result)

    def with_celery_options(self, **kwargs: Any) -> "Task[J]":
        return Task(self.signature.clone().set(**kwargs))


@types.coroutine
def _parallel(
    tasks: Sequence[Task[JSON]],
) -> Generator[list[CelerySignature], list[JSON], tuple[JSON, ...]]:
    results = yield [task.signature for task in tasks]
    return typing.cast(tuple[JSON], tuple(results))


J1 = TypeVar("J1", bound=JSON, covariant=True)
J2 = TypeVar("J2", bound=JSON, covariant=True)
J3 = TypeVar("J3", bound=JSON, covariant=True)


@overload
async def parallel(tasks: tuple[Task[J1], Task[J2]]) -> tuple[J1, J2]:
    ...


@overload
async def parallel(tasks: tuple[Task[J1], Task[J2], Task[J3]]) -> tuple[J1, J2, J3]:
    ...


# etc., could add more arities.


@overload
async def parallel(tasks: Sequence[Task[J]]) -> tuple[J, ...]:
    ...


@overload
async def parallel(tasks: Mapping[str, Task[J]]) -> dict[str, J]:
    ...


async def parallel(
    tasks: Sequence[Task[Any]] | Mapping[str, Task[Any]]
) -> Sequence[Any] | Mapping[str, Any]:
    if isinstance(tasks, Sequence):
        return await _parallel(tasks)
    return dict(zip(tasks.keys(), await _parallel(list(tasks.values()))))


P = ParamSpec("P")


# I tried writing this as a simple @types.coroutine, but mypy didn't like it.
class side_effect(Generic[J]):
    def __init__(self, f: Callable[P, J], *args: P.args, **kwargs: P.kwargs) -> None:
        self.f = f
        self.args = args
        self.kwargs = kwargs

    # Again not sure why mypye wants me to use Any here 🤔
    def __await__(self) -> Generator[Thunk[J], Any, J]:
        # Yield a thunk of the effectful computation so that the loop can
        # memoize its return value.
        [result] = yield lambda: self.f(*self.args, **self.kwargs)
        return result


@dataclass
class Ok(Generic[T]):
    value: T


class TaskError(Exception):
    """
    Think there might be a nicer way to do this in celery, but for now, lump all
    task errors into a common type.
    """
    def __init__(self, name: str, exc: str, traceback: str) -> None:
        self.name = name
        self.exc = exc
        self.traceback = traceback
        super().__init__(f"TaskError: {name} raised {exc}:\n{traceback}")


celeryapp = celery.Celery(
    "scelerat",
    broker="amqp://guest:guest@localhost:5672//",
    # Use a results backend so that we can use chords for parallel tasks.
    backend="redis://localhost",
    result_extended=True,
    task_remote_tracebacks=True,
)


#
# Use TypedDicts so that they still count as JSON.
# Or maybe I should just use pydantic.
#


class Done(TypedDict, Generic[J]):
    result: J


class SingleTaskResult(TypedDict):
    kind: Literal["single"]
    result: JSON


class ParallelTaskResult(TypedDict):
    kind: Literal["parallel"]
    result: list[JSON]


class TaskException(TypedDict):
    kind: Literal["exception"]
    name: str
    exc: str
    traceback: str


TaskResponse = SingleTaskResult | ParallelTaskResult | TaskException


# Ugh
class NOTSET:
    pass


notset = NOTSET()

#
# ✨ The main recursive task ✨
#


@celeryapp.task(queue="scelerat", name="scelerat.run_workflow")
def run_workflow(
    # `latest_step_result` is usually fed in by a celery chain/chord.
    latest_step_result: JSON,
    latest_step_result_kind: Literal["single", "parallel", "exception"],
    history: list[TaskResponse | None],
    name: str,
    args: list[JSON],
    kwargs: dict[str, JSON],
) -> Done[JSON] | None:
    """
    Receives the response to a workflow's most recent request (e.g. to do a
    task) and advances the workflow one step, recursing if the workflow still
    has more work to do.

    Keeps track of every prior step's response in `history`.
    """

    import sys

    # Hackily look up the workflow by name.
    workflow = getattr(sys.modules[__name__], name)(*args, **kwargs)

    def _amended_history() -> list[TaskResponse | None]:
        if len(history) == 0 and latest_step_result_kind == "single":
            assert (
                latest_step_result is None
            ), f"Expected None, got {latest_step_result}"
            return [None]

        match latest_step_result_kind:
            case "single":
                return history + [
                    SingleTaskResult(kind="single", result=latest_step_result)
                ]
            case "parallel":
                assert isinstance(latest_step_result, list)
                return history + [
                    ParallelTaskResult(kind="parallel", result=latest_step_result)
                ]
            case "exception":
                # Weirdly, we'll get passed the failed task's task id here.
                # Gotta be an easier way to do this, but for now we have to
                # go look up the task's result (aka failure).
                assert isinstance(latest_step_result, str)
                from celery.result import AsyncResult, allow_join_result

                with allow_join_result():
                    task_result = AsyncResult(latest_step_result)
                    print(f"{task_result.state=}")
                    try:
                        task_result.get(propagate=True)
                    except Exception as e:
                        return history + [
                            TaskException(
                                kind="exception",
                                name=task_result.name,
                                exc=str(e),
                                traceback=str(e.__traceback__),
                            )
                        ]
                    else:
                        raise AssertionError("Shouldn't get here")
            case _:
                raise ValueError(f"Unknown result kind: {latest_step_result_kind}")

    amended_history = _amended_history()
    print(f"{amended_history=}")

    def recurse(
        kind: Literal["single", "parallel", "exception"],
        latest_step_result: JSON | NOTSET = notset,
    ) -> CelerySignature:
        return run_workflow.s(
            **(
                {}
                if latest_step_result is notset
                else dict(latest_step_result=latest_step_result)
            ),
            latest_step_result_kind=kind,
            history=amended_history,
            name=name,
            args=args,
            kwargs=kwargs,
        ).set(queue="scelerat")

    # Replay what's happened so far, and then decide what to do next.
    continuation: CelerySignature
    match replay(workflow, history=amended_history):
        case Ok(value):  # All done!
            return Done(result=value)
        case []:  # No tasks to do
            # Just directly specify the `latest_step_result`.
            continuation = recurse(kind="parallel", latest_step_result=[])
        case [task]:  # A single task, no need for parallelism
            # Chain the next step and then recurse with its result.
            continuation = task.signature.on_error(recurse(kind="exception")) | recurse(
                kind="single"
            )
        case [*tasks]:  # Multiple tasks, so use a chord
            # Group the next steps so they run in parallel and then recurse with their results.
            # The `on_error` syntax looks weird but that seems to be how you handle errors
            # from the individual sub tasks 🤷‍♂️
            continuation = celery.group([task.signature for task in tasks]) | recurse(
                kind="parallel"
            ).on_error(recurse(kind="exception"))

        case thunk if callable(thunk):
            continuation = recurse(kind="single", latest_step_result=thunk())
        case _:
            raise AssertionError("Unexpected return value from replay")

    continuation.apply_async()
    return None


def replay(
    workflow: Workflow[J], *, history: list[TaskResponse | None]
) -> Ok[J] | list[Task[JSON]] | Thunk[JSON]:
    """Feed a generator with values from a history."""

    # Feels weird, but it seems simplest to just store the generator's initial
    # None in the same history as everything else.
    assert len(history) > 0, "History must be non-empty"
    assert history[0] is None, "History must start with None"

    todo: list[CelerySignature] | Thunk[JSON] = []
    # Would be interesting to also be able to send the workflow exceptions
    # raised along the way, but that requires serializing them somehow.
    for i, value in enumerate(history):
        try:
            if value is None:
                todo = workflow.send(None)
                continue

            match value["kind"]:
                case "single":
                    todo = workflow.send([value["result"]])  # type: ignore
                case "parallel":
                    todo = workflow.send(value["result"])  # type: ignore
                case "exception":
                    print("About to throw back into workflow", value, history)
                    todo = workflow.throw(
                        TaskError(
                            name=value["name"],  # type: ignore
                            exc=value["exc"],  # type: ignore
                            traceback=value["traceback"],  # type: ignore
                        )
                    )
        except StopIteration as ret:
            assert i == len(history) - 1
            return Ok(ret.value)

    if isinstance(todo, list):
        return [Task(sig) for sig in todo]
    return todo


def apply_workflow_async(
    workflow: Callable[P, Workflow[JSON]], *args: P.args, **kwargs: P.kwargs
) -> None:
    """Kick off a workflow."""
    run_workflow.s(
        latest_step_result=None,
        latest_step_result_kind="single",
        history=[],
        name=workflow.__name__,
        args=args,
        kwargs=kwargs,
    ).set(queue="scelerat").apply_async()


# Helper tasks etc.


def get_url(url: str) -> Task[str]:
    return Task(celery.signature("lightweight_http.request", args=[url], queue="http"))


def boom(arg: int) -> Task[None]:
    return Task(celery.signature("lightweight_http.boom", args=[arg], queue="http"))


@celeryapp.task
def noop() -> None:
    pass


def sleep(seconds: float) -> Task[None]:
    return Task(noop.s().set(queue="scelerat", countdown=seconds))
