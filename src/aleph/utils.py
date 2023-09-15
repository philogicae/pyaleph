import asyncio
import concurrent.futures
from hashlib import sha256
from typing import Union, TypeVar, Callable, Optional, TypeVarTuple, ParamSpec

from aleph_message.models import ItemType

from aleph.exceptions import UnknownHashError
from aleph.settings import settings

T = TypeVar("T")
# TODO: use TypeVarTuple and (func: Callable[*Ts, T], *args: *Ts) once supported by mypy
P = ParamSpec("P")
Ts = TypeVarTuple("Ts")


async def run_in_executor(
    executor: Optional[concurrent.futures.Executor],
    func: Callable[P, T],
    *args: P.args,
    **kwargs: P.kwargs,
) -> T:
    # We have to specify kwargs because of ParamSpec, but it is not supported with run_in_executor.
    if kwargs:
        raise ValueError("Do not use kwargs with run_in_executor, it is unsupported.")

    if settings.use_executors:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(executor, func, *args)
    else:
        return func(*args)


def item_type_from_hash(item_hash: str) -> ItemType:
    # https://docs.ipfs.io/concepts/content-addressing/#identifier-formats
    if item_hash.startswith("Qm") and 44 <= len(item_hash) <= 46:  # CIDv0
        return ItemType.ipfs
    elif item_hash.startswith("bafy") and len(item_hash) == 59:  # CIDv1
        return ItemType.ipfs
    elif len(item_hash) == 64:
        return ItemType.storage
    else:
        raise UnknownHashError(f"Unknown hash {len(item_hash)} {item_hash}")


def get_sha256(content: Union[str, bytes]) -> str:
    if isinstance(content, str):
        content = content.encode("utf-8")
    return sha256(content).hexdigest()
