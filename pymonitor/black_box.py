import sys
import os
import time


def consume_memory(multiplier, repeats):
    step = os.urandom(multiplier * 1024 * 1024)
    memory = b''
    while repeats:
        memory += step
        repeats -= 1
        time.sleep(1)


if __name__ == '__main__':
    multiplier = int(sys.argv[1])
    repeats = int(sys.argv[2])

    consume_memory(multiplier, repeats)
