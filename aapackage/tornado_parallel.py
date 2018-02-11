#!/usr/bin/env python2.7
# -*- coding=utf-8 -*-

import shlex
import subprocess
from tornado.gen import coroutine, Task, Return, sleep
from tornado.process import Subprocess
from tornado.ioloop import IOLoop


@coroutine
def call_subprocess(cmd, stdin_data=None, stdin_async=True):
    """call sub process async

        Args:
            cmd: str, commands
            stdin_data: str, data for standard in
            stdin_async: bool, whether use async for stdin
    """
    stdin = Subprocess.STREAM if stdin_async else subprocess.PIPE
    sub_process = Subprocess(shlex.split(cmd),
                             stdin=stdin,
                             stdout=Subprocess.STREAM,
                             stderr=Subprocess.STREAM, )
    if stdin_data:
        if stdin_async:
            yield Task(sub_process.stdin.write, stdin_data)
        else:
            sub_process.stdin.write(stdin_data)

    if stdin_async or stdin_data:
        sub_process.stdin.close()

    result, error = yield [Task(sub_process.stdout.read_until_close),
                           Task(sub_process.stderr.read_until_close), ]

    raise Return((result.decode(), error.decode()))


@coroutine
def example_main():
    all_command = list()
    all_command.append(call_subprocess("ls /home/"))
    all_command.append(call_subprocess("ls -l", stdin_data=b"/home/"))
    all_command.append(call_subprocess("ls -l |tail -2"))

    while all_command:
        for comm in all_command:
            if comm.done():
                print(comm.result())
                all_command.remove(comm)
        yield sleep(0.5)

    IOLoop.instance().stop()


if __name__ == "__main__":
    IOLoop.instance().add_callback(example_main)
    IOLoop.instance().start()
