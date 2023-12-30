import time
import logging

from icecream import ic
from multiprocessing import Pool

logger = logging.getLogger()
stream_handler = logging.StreamHandler()
logger.addHandler(stream_handler)
logger.setLevel(logging.DEBUG)
"""
Друга частина для процесів
Напишіть реалізацію функції factorize, яка приймає список чисел та повертає
список чисел, на які числа з вхідного списку поділяються без залишку.

Реалізуйте синхронну версію та виміряйте час виконання.

Потім покращіть продуктивність вашої функції, реалізувавши використання
кількох ядер процесора для паралельних обчислень і замірьте час виконання
знову. Для визначення кількості ядер на машині використовуйте функцію
cpu_count() з пакета multiprocessing

Для перевірки правильності роботи алгоритму самої функції можете
скористатися тестом:
"""


def factorize_simple(*number):

    result = {}

    for num in number:
        checker = 1
        temp = []

        while checker <= num:
            if not num % checker:
                temp.append(checker)
            checker += 1

        result |= {num: temp}

    ic(result)

    return result.values()


def factorize_process(*number):

    result = {}

    for num in number:
        checker = 1
        temp = []

        while checker <= num:
            if not num % checker:
                temp.append(checker)
            checker += 1

        result[num] = temp

    return [x for values in result.values() for x in values]


if __name__ == '__main__':

    start_time = time.time()

    a, b, c, d = factorize_simple(128, 255, 99999, 10651060)

    end_time = time.time()
    elapsed_time = end_time - start_time
    ic(elapsed_time)

    assert a == [1, 2, 4, 8, 16, 32, 64, 128], f"{a = }"
    assert b == [1, 3, 5, 15, 17, 51, 85, 255], f"{b = }"
    assert c == [1, 3, 9, 41, 123, 271, 369, 813, 2439, 11111, 33333, 99999], f"{c = }"
    assert d == [1, 2, 4, 5, 7, 10, 14, 20, 28, 35, 70, 140, 76079, 152158, 304316, 380395, 532553, 760790, 1065106, 1521580, 2130212, 2662765, 5325530, 10651060], f"{d = }"

    start_time = time.time()

    with Pool(processes=4) as pool:
        a, b, c, d = pool.map(factorize_process, (128, 255, 99999, 10651060))

    end_time = time.time()
    elapsed_time = end_time - start_time
    ic(elapsed_time)

    assert a == [1, 2, 4, 8, 16, 32, 64, 128], f"{a = }"
    assert b == [1, 3, 5, 15, 17, 51, 85, 255], f"{b = }"
    assert c == [1, 3, 9, 41, 123, 271, 369, 813, 2439, 11111, 33333, 99999], f"{c = }"
    assert d == [1, 2, 4, 5, 7, 10, 14, 20, 28, 35, 70, 140, 76079, 152158, 304316, 380395, 532553, 760790, 1065106, 1521580, 2130212, 2662765, 5325530, 10651060], f"{d = }"
