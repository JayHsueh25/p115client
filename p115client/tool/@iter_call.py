#!/usr/bin/env python3
# encoding: utf-8

# TODO: 这个模块可以单独拎出来，作为一个公共模块 iter_call.py，而脱离 p115client 的约束

__author__ = "ChenyangGao <https://chenyanggao.github.io>"
__all__ = ["iter_call", "iter_page_call", "iter_next_call"]

from asyncio import (
    shield, sleep as async_sleep, wait_for, 
    Semaphore as AsyncSemaphore, Task, TaskGroup, 
)
from collections import deque
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator
from concurrent.futures import Future, ThreadPoolExecutor
from copy import copy
from os import PathLike
from time import sleep, time
from typing import overload, Any, Literal
from warnings import warn

from errno2 import errno
from http_response import get_status_code, is_timeouterror
from iterutils import run_gen_step_iter, Yield
from p115client import check_response, P115Client, P115OpenClient
from p115client.exception import throw, P115DataError, P115Warning


@overload
def iter_call(
    client: str | PathLike | P115Client | P115OpenClient, 
    payload: int | str | dict = 0, 
    /, 
    page_size: int = 7_000, 
    first_page_size: int = 0, 
    count: int = -1, 
    callback: None | Callable[[dict], Any] = None, 
    app: str = "web", 
    raise_for_changed_count: bool = False, 
    cooldown: None | float = None, 
    max_workers: None | int = None, 
    *, 
    async_: Literal[False] = False, 
    **request_kwargs, 
) -> Iterator[dict]:
    ...
@overload
def iter_call(
    client: str | PathLike | P115Client | P115OpenClient, 
    payload: int | str | dict = 0, 
    /, 
    page_size: int = 7_000, 
    first_page_size: int = 0, 
    count: int = -1, 
    callback: None | Callable[[dict], Any] = None, 
    app: str = "web", 
    raise_for_changed_count: bool = False, 
    cooldown: None | float = None, 
    max_workers: None | int = None, 
    *, 
    async_: Literal[True], 
    **request_kwargs, 
) -> AsyncIterator[dict]:
    ...
def iter_call(
    client: str | PathLike | P115Client | P115OpenClient, 
    payload: int | str | dict = 0, 
    /, 
    page_size: int = 7_000, 
    first_page_size: int = 0, 
    count: int = -1, 
    callback: None | Callable[[dict], Any] = None, 
    app: str = "web", 
    raise_for_changed_count: bool = False, 
    cooldown: None | float = None, 
    max_workers: None | int = None, 
    *, 
    async_: Literal[False, True] = False, 
    **request_kwargs, 
) -> AsyncIterator[dict] | Iterator[dict]:
    """拉取一个目录中的文件或目录的数据

    :param client: 115 网盘客户端对象
    :param payload: 目录的 id 或者详细的查询参数
    :param page_size: 分页大小，如果 <= 0，则自动确定
    :param first_page_size: 首次拉取的分页大小，如果 <= 0，则自动确定
    :param count: 文件总数
    :param callback: 回调函数，调用后，会获得一个值，会添加到返回值中，key 为 "callback"
    :param app: 使用此设备的接口
    :param raise_for_changed_count: 分批拉取时，发现总数发生变化后，是否报错
    :param cooldown: 冷却时间，单位为秒。如果为 None，则用默认值（非并发时为 0，并发时为 1）
    :param max_workers: 最大并发数，如果为 None 或 < 0 则自动确定，如果为 0 则单工作者惰性执行
    :param async_: 是否异步
    :param request_kwargs: 其它 http 请求参数，会传给具体的请求函数，默认的是 httpx，可用参数 request 进行设置

    :return: 迭代器，每次返回一次接口调用的结果
    """
    if max_workers == 0:
        request_kwargs["async_"] = async_
        method: Callable = iter_fs_files_serialized
    else:
        request_kwargs["max_workers"] = max_workers
        if async_:
            method = iter_fs_files_asynchronized
        else:
            method = iter_fs_files_threaded
    if cooldown is not None:
        request_kwargs["cooldown"] = cooldown
    return method(
        client, 
        payload, 
        page_size=page_size, 
        first_page_size=first_page_size, 
        count=count, 
        callback=callback, 
        app=app, 
        raise_for_changed_count=raise_for_changed_count, 
        **request_kwargs, 
    )


@overload
def serial_iter_call(
    client: str | PathLike | P115Client | P115OpenClient, 
    payload: int | str | dict = 0, 
    /, 
    page_size: int = 7_000, 
    first_page_size: int = 0, 
    count: int = -1, 
    callback: None | Callable[[dict], Any] = None, 
    app: str = "web", 
    raise_for_changed_count: bool = False, 
    cooldown: float = 0, 
    *, 
    async_: Literal[False] = False, 
    **request_kwargs, 
) -> Iterator[dict]:
    ...
@overload
def serial_iter_call(
    client: str | PathLike | P115Client | P115OpenClient, 
    payload: int | str | dict = 0, 
    /, 
    page_size: int = 7_000, 
    first_page_size: int = 0, 
    count: int = -1, 
    callback: None | Callable[[dict], Any] = None, 
    app: str = "web", 
    raise_for_changed_count: bool = False, 
    cooldown: float = 0, 
    *, 
    async_: Literal[True], 
    **request_kwargs, 
) -> AsyncIterator[dict]:
    ...
def serial_iter_call(
    client: str | PathLike | P115Client | P115OpenClient, 
    payload: int | str | dict = 0, 
    /, 
    page_size: int = 7_000, 
    first_page_size: int = 0, 
    count: int = -1, 
    callback: None | Callable[[dict], Any] = None, 
    app: str = "web", 
    raise_for_changed_count: bool = False, 
    cooldown: float = 0, 
    *, 
    async_: Literal[False, True] = False, 
    **request_kwargs, 
) -> AsyncIterator[dict] | Iterator[dict]:
    """拉取一个目录中的文件或目录的数据

    :param client: 115 网盘客户端对象
    :param payload: 目录的 id 或者详细的查询参数
    :param page_size: 分页大小，如果 <= 0，则自动确定
    :param first_page_size: 首次拉取的分页大小，如果 <= 0，则自动确定
    :param count: 文件总数
    :param callback: 回调函数，调用后，会获得一个值，会添加到返回值中，key 为 "callback"
    :param app: 使用此设备的接口
    :param raise_for_changed_count: 分批拉取时，发现总数发生变化后，是否报错
    :param cooldown: 冷却时间，单位为秒
    :param async_: 是否异步
    :param request_kwargs: 其它 http 请求参数，会传给具体的请求函数，默认的是 httpx，可用参数 request 进行设置

    :return: 迭代器，每次返回一次接口调用的结果
    """
    if isinstance(client, (str, PathLike)):
        client = P115Client(client, check_for_relogin=True)
    if page_size <= 0:
        page_size = 7_000
    fs_files: Callable
    if not isinstance(client, P115Client) or app == "open":
        page_size = min(page_size, 1150)
        fs_files = client.fs_files_open
    elif app in ("", "web", "desktop", "harmony"):
        page_size = min(page_size, 1150)
        request_kwargs.setdefault("base_url", get_webapi_origin)
        fs_files = client.fs_files
    elif app == "aps":
        page_size = min(page_size, 1200)
        fs_files = client.fs_files_aps
    else:
        request_kwargs.setdefault("base_url", get_proapi_origin)
        request_kwargs["app"] = app
        fs_files = client.fs_files_app
    if first_page_size <= 0:
        first_page_size = page_size
    if isinstance(payload, (int, str)):
        payload = {"cid": client.to_id(payload)}
    payload = {
        "asc": 1, "cid": 0, "fc_mix": 1, "o": "user_ptime", "offset": 0, 
        "limit": first_page_size, "show_dir": 1, **payload, 
    }
    cid = int(payload["cid"])
    def gen_step():
        nonlocal count, first_page_size
        last_call_ts: float = 0
        while True:
            while True:
                try:
                    if cooldown > 0 and (delta := last_call_ts + cooldown - time()) > 0:
                        if async_:
                            yield async_sleep(delta)
                        else:
                            sleep(delta)
                        last_call_ts = time()
                    resp = yield fs_files(payload, async_=async_, **request_kwargs)
                    check_response(resp)
                except P115DataError:
                    if payload["limit"] <= 1150:
                        raise
                    payload["limit"] -= 1_000
                    if payload["limit"] < 1150:
                        payload["limit"] = 1150
                    continue
                if cid and int(resp["path"][-1]["cid"]) != cid:
                    if count < 0:
                        throw(errno.ENOTDIR, cid)
                    else:
                        throw(errno.ENOENT, cid)
                count_new = int(resp["count"])
                if count < 0:
                    count = count_new
                elif count != count_new:
                    message = f"cid={cid} detected count changes during iteration: {count} -> {count_new}"
                    if raise_for_changed_count:
                        throw(errno.EBUSY, message)
                    else:
                        warn(message, category=P115Warning)
                    count = count_new
                if callback is not None:
                    resp["callback"] = yield callback(resp)
                break
            yield Yield(resp)
            payload["offset"] += len(resp["data"])
            if payload["offset"] >= count:
                break
            if first_page_size != page_size:
                payload["limit"] = page_size
                first_page_size = page_size
    return run_gen_step_iter(gen_step, async_)


def thread_iter_call(
    client: str | PathLike | P115Client | P115OpenClient, 
    payload: int | str | dict = 0, 
    /, 
    page_size: int = 7_000, 
    first_page_size: int = 0, 
    count: int = -1, 
    callback: None | Callable[[dict], Any] = None, 
    app: str = "web", 
    raise_for_changed_count: bool = False, 
    cooldown: float = 1, 
    max_workers: None | int = None, 
    **request_kwargs, 
) -> Iterator[dict]:
    """多线程并发拉取一个目录中的文件或目录的数据

    :param client: 115 网盘客户端对象
    :param payload: 目录的 id、pickcode 或者详细的查询参数
    :param page_size: 分页大小，如果 <= 0，则自动确定
    :param first_page_size: 第 1 次拉取的分页大小，如果指定此参数，则会等待这次请求返回，才会开始后续，也即非并发
    :param count: 文件总数
    :param callback: 回调函数，调用后，会获得一个值，会添加到返回值中，key 为 "callback"
    :param app: 使用此设备的接口
    :param raise_for_changed_count: 分批拉取时，发现总数发生变化后，是否报错
    :param cooldown: 冷却时间，单位为秒
    :param max_workers: 最大工作线程数，如果为 None，则自动确定
    :param request_kwargs: 其它 http 请求参数，会传给具体的请求函数，默认的是 httpx，可用参数 request 进行设置

    :return: 迭代器
    """
    if isinstance(client, (str, PathLike)):
        client = P115Client(client, check_for_relogin=True)
    if page_size <= 0:
        page_size = 7_000
    fs_files: Callable[..., Awaitable[dict]]
    if not isinstance(client, P115Client) or app == "open":
        page_size = min(page_size, 1150)
        fs_files = client.fs_files_open
    elif app in ("", "web", "desktop", "harmony"):
        page_size = min(page_size, 1150)
        request_kwargs.setdefault("base_url", get_webapi_origin)
        fs_files = client.fs_files
    elif app == "aps":
        page_size = min(page_size, 1200)
        fs_files = client.fs_files_aps
    else:
        request_kwargs.setdefault("base_url", get_proapi_origin)
        request_kwargs["app"] = app
        fs_files = client.fs_files_app
    if isinstance(payload, (int, str)):
        payload = {"cid": client.to_id(payload)}
    if first_page_size <= 0:
        first_page_size = page_size
    payload = {
        "asc": 1, "cid": 0, "fc_mix": 1, "o": "user_ptime", "offset": 0, 
        "limit": first_page_size, "show_dir": 1, **payload, 
    }
    cid = int(payload["cid"])
    def get_files(payload: dict, /):
        nonlocal count
        resp = fs_files(payload, **request_kwargs)
        check_response(resp)
        if cid and int(resp["path"][-1]["cid"]) != cid:
            if count < 0:
                throw(errno.ENOTDIR, cid)
            else:
                throw(errno.ENOENT, cid)
        count_new = int(resp["count"])
        if count < 0:
            count = count_new
        elif count != count_new:
            message = f"cid={cid} detected count changes during iteration: {count} -> {count_new}"
            if raise_for_changed_count:
                throw(errno.EBUSY, message)
            else:
                warn(message, category=P115Warning)
            count = count_new
        if callback is not None:
            resp["callback"] = callback(resp)
        return resp
    dq: deque[tuple[Future, int]] = deque()
    push, pop = dq.append, dq.popleft
    if max_workers is not None and max_workers <= 0:
        max_workers = None
    executor = ThreadPoolExecutor(max_workers=max_workers)
    submit = executor.submit
    last_call_ts: int | float = 0
    def make_future(args: None | dict = None, /) -> Future:
        nonlocal last_call_ts
        if args is None:
            args = copy(payload)
        last_call_ts = time()
        return submit(get_files, args)
    try:
        future = make_future()
        payload["limit"] = page_size
        offset = payload["offset"]
        while True:
            try:
                if first_page_size == page_size:
                    resp = future.result(max(0, last_call_ts + cooldown - time()))
                else:
                    resp = future.result()
                    first_page_size = page_size
            except TimeoutError:
                payload["offset"] += page_size
                if count < 0 or payload["offset"] < count:
                    push((make_future(), payload["offset"]))
            except BaseException as e:
                if get_status_code(e) >= 400 or not is_timeouterror(e):
                    raise
                future = make_future({**payload, "offset": offset})
            else:
                yield resp
                reach_end = offset != resp["offset"] or count >= 0 and offset + len(resp["data"]) >= count
                will_continue = False
                while dq:
                    future, offset = pop()
                    if count < 0 or offset < count:
                        will_continue = True
                        break
                    future.cancel()
                if will_continue:
                    continue
                elif reach_end:
                    break
                else:
                    offset = payload["offset"] = payload["offset"] + page_size
                    if count >= 0 and offset >= count:
                        break
                    future = make_future()
    finally:
        executor.shutdown(False, cancel_futures=True)


# NOTE: 有时，分页大小是定死的，但你也要传，以作为一个评判依据
# NOTE: 特别的，所有接口，如果返回的数据数是 0，则肯定没有下一页，这也是一个重要的判断依据（此时就不用管分页大小是多少了）
async def async_iter_call[T](
    method: Callable[..., Awaitable[T]], 
    /, 
    start: int = 0, 
    stop: None | int = None, 
    step: int | Callable[[], int] = 100, 
    payload: None | dict[str, Any] = None, 
    payload_offset: str = "offset", 
    payload_limit: str = "limit", 
    response_count: None | int | str | Callable[[dict], int | str] = "count", 
    response_data: None | str | Callable[[dict], int | str | Collection] = "data", 
    response_next: None | str | Callable[[dict, dict], SupportsBool] = None, 
    raise_for_changed_count: None | BaseException | type[BaseException] = None, 
    cooldown: float | Callable[[dict], float] = 0, 
    max_workers: None | int = None, 
    **request_kwargs, 
) -> AsyncIterator[T]:
    """异步并发调用一个接口，然后返回数据

    :param method: 接口方法，调用以获取响应
    :param start: 开始索引，从 0 开始计
    :param stop: 结束索引（不含）
    :param step: 步进索引，也即分页大小。如果是调用，则调用以获取一个步进值
    :param payload: 查询参数字典
    :param payload_offset: 向 `payload` 中设置偏移时，所用字段
    :param payload_limit: 向 `payload` 中设置分页大小时，所用字段
    :param response_count: 给出数据总数，或者从响应中获取数据总数，可用来判断是否有下一页
    :param response_data: 从响应中获取数据，或者数据的个数，如果为 None 或者调用后抛出 LookupError，则忽略；当可用时，可用来和 `step` 作对比，不相等时认为无下一页
    :param response_next: 断言一个响应是否有下一页（如果这个响应值不可用，可在里面抛出异常，如果抛出 `StopIteration`，则静默退出）
    :param raise_for_changed_count: 分批拉取时，发现总数发生变化后，报什么错
    :param cooldown: 冷却时间，单位是秒。如果得到的值小于等于 0，则不冷却；如果是调用，则会把具体的 payload 传给它，再返回冷却时间
    :param max_workers: 最大工作协程数，如果为 None 或 <= 0，则为 16
    :param request_kwargs: 其它请求参数，会直接传给 `method`

    :return: 异步迭代器
    """
    if payload is None:
        payload = {}


    if max_workers is None or max_workers <= 0:
        max_workers = 64
    sema = AsyncSemaphore(max_workers)
    async def get_files(payload: dict, /):
        async with sema:
            resp = await method(payload, async_=True, **request_kwargs)
        check_response(resp)
        if cid and int(resp["path"][-1]["cid"]) != cid:
            if count < 0:
                throw(errno.ENOTDIR, cid)
            else:
                throw(errno.ENOENT, cid)
        
        count_new = int(resp["count"])
        if count < 0:
            count = count_new
        elif count != count_new:
            message = f"cid={cid} detected count changes during iteration: {count} -> {count_new}"
            if raise_for_changed_count:
                throw(errno.EBUSY, message)
            else:
                warn(message, category=P115Warning)
            count = count_new
        return resp
    dq: deque[tuple[Task, int]] = deque()
    push, pop = dq.append, dq.popleft
    async with TaskGroup() as tg:
        create_task = tg.create_task
        last_call_ts: float = 0
        def make_task(args: None | dict = None, /) -> Task:
            nonlocal last_call_ts
            if args is None:
                args = copy(payload)
            last_call_ts = time()
            return create_task(get_files(args))
        task   = make_task()
 
        payload[field_offset] = offset
        payload[field_limit]  = step

        count = -1
        while True:
            try:
                if first_step == step:
                    resp = await wait_for(shield(task), max(0, last_call_ts + cooldown - time()))
                else:
                    resp = await task
                    first_page_size = page_size
            except TimeoutError:
                payload[field_offset] += step
                if count < 0 or payload[field_offset] < count:
                    push((make_task(), payload[field_offset]))
            except BaseException as e:
                if get_status_code(e) >= 400 or not is_timeouterror(e):
                    raise
                task = make_task({**payload, "offset": offset})
            else:
                yield resp
                reach_end = offset != resp[field_offset] or count >= 0 and offset + len(resp["data"]) >= count
                will_continue = False
                while dq:
                    task, offset = pop()
                    if count < 0 or offset < count:
                        will_continue = True
                        break
                    task.cancel()
                if will_continue:
                    continue
                elif reach_end:
                    break
                else:
                    offset = payload["offset"] = payload["offset"] + page_size
                    if count >= 0 and offset >= count:
                        break
                    task = make_task()


# NOTE: 有时，分页大小是定死的，但你也要传，以作为一个评判依据
# NOTE: 特别的，所有接口，如果返回的数据数是 0，则肯定没有下一页，这也是一个重要的判断依据（此时就不用管分页大小是多少了）
async def async_iter_page_call[T](
    method: Callable[..., Awaitable[T]], 
    /, 
    page: int = 0, 
    page_size: int = 100, 
    payload: None | dict[str, Any] = None, 
    payload_page: str = "page", 
    payload_page_size: str = "page_size", 
    response_count: None | int | str | Callable[[dict], int | str] = "count", 
    response_data: None | str | Callable[[dict], int | str | Collection] = "data", 
    response_next: None | str | Callable[[dict, dict], SupportsBool] = None, 
    raise_for_changed_count: None | BaseException | type[BaseException] = None, 
    cooldown: float | Callable[[dict], float] = 0, 
    max_workers: None | int = None, 
    **request_kwargs, 
) -> AsyncIterator[T]:
    """异步并发调用一个接口，然后返回数据

    :param method: 接口方法，调用以获取响应
    :param page: 页数，从 1 开始计
    :param page_size: 分页大小
    :param payload: 查询参数字典
    :param payload_page: 向 `payload` 中设置页数时，所用字段
    :param payload_page_size: 向 `payload` 中设置分页大小时，所用字段
    :param response_count: 给出数据总数，或者从响应中获取数据总数，可用来判断是否有下一页
    :param response_data: 从响应中获取数据，或者数据的个数，如果为 None 或者调用后抛出 LookupError，则忽略；当可用时，可用来和 `step` 作对比，不相等时认为无下一页
    :param response_next: 断言一个响应是否有下一页（如果这个响应值不可用，可在里面抛出异常，如果抛出 `StopIteration`，则静默退出）
    :param raise_for_changed_count: 分批拉取时，发现总数发生变化后，报什么错
    :param cooldown: 冷却时间，单位是秒。如果得到的值小于等于 0，则不冷却；如果是调用，则会把具体的 payload 传给它，再返回冷却时间
    :param max_workers: 最大工作协程数，如果为 None 或 <= 0，则为 16
    :param request_kwargs: 其它请求参数，会直接传给 `method`

    :return: 异步迭代器
    """


# TODO: 或许也可以指定下一页的分页大小（可以忽略）
# NOTE: 有时，分页大小是定死的，但你也要传，以作为一个评判依据
# NOTE: 特别的，所有接口，如果返回的数据数是 0，则肯定没有下一页，这也是一个重要的判断依据（此时就不用管分页大小是多少了）
async def async_iter_next_call[T](
    method: Callable[..., Awaitable[T]], 
    /, 
    payload: None | dict[str, Any] = None, 
    payload_next: str = "next", 
    response_count: None | int | str | Callable[[dict], int | str] = "count", 
    response_next: None | str | Callable[[dict], Any] = "next", 
    raise_for_changed_count: None | BaseException | type[BaseException] = None, 
    cooldown: float | Callable[[dict], float] = 0, 
    max_workers: None | int = None, 
    **request_kwargs, 
) -> AsyncIterator[T]:
    """异步并发调用一个接口，然后返回数据

    :param method: 接口方法，调用以获取响应
    :param payload: 查询参数字典
    :param payload_next: 向 `payload` 中设置下一页的标记时，所用字段
    :param response_count: 给出数据总数，或者从响应中获取数据总数
    :param response_next: 从响应中获取下一页的标记
    :param raise_for_changed_count: 分批拉取时，发现总数发生变化后，报什么错
    :param cooldown: 冷却时间，单位是秒。如果得到的值小于等于 0，则不冷却；如果是调用，则会把具体的 payload 传给它，再返回冷却时间
    :param max_workers: 最大工作协程数，如果为 None 或 <= 0，则为 16
    :param request_kwargs: 其它请求参数，会直接传给 `method`

    :return: 异步迭代器
    """


# TODO: 对外暴露，max_workers=0，则是序列模式
# TODO: 支持 3 种模式，1) offset 模式(offset+limit)，2) page 模式(page+page_size)，3) next 模式(next_marker)

# TODO: 如何判断是否有下一页：1. 响应中有没有 count，如果有就用，如果没有，那么查看数据返回条数，如果不等于一页的大小，就进行停止，判断停止的条件（count, pagesize, stopmark）
# TODO: 判断是否有下一页：1. 看下一次的偏移和 count 对比，2. 这一页的大小和 pagesize 对比 3. 是否有结束标记

# TODO: 再支持一种参数，shuffle，允许结果以乱序返回

