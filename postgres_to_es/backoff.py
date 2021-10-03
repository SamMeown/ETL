from functools import wraps
import time
from typing import List
import logging


def backoff(exceptions: List, start_sleep_time=0.1, factor=2, border_sleep_time=10, total_sleep_time=30):
    """
    Функция для повторного выполнения функции через некоторое время, если возникла ошибка. Использует наивный
    экспоненциальный рост времени повтора (factor) до граничного времени ожидания (border_sleep_time)

    Формула:
        t = start_sleep_time * 2^(n) if t < border_sleep_time
        t = border_sleep_time if t >= border_sleep_time
    :param exceptions: перехватываемые ошибки, на которые будем делать повторы
    :param start_sleep_time: начальное время повтора
    :param factor: во сколько раз нужно увеличить время ожидания
    :param border_sleep_time: граничное время ожидания
    :param total_sleep_time: максимальное время, выделенное на все повторы
    :return: результат выполнения функции
    """

    def func_wrapper(func):
        total_sleep_left = total_sleep_time
        sleep_time = min(start_sleep_time, total_sleep_left)

        @wraps(func)
        def inner(*args, **kwargs):
            nonlocal sleep_time, total_sleep_left
            try:
                result = func(*args, **kwargs)
                total_sleep_left = total_sleep_time
                sleep_time = min(start_sleep_time, total_sleep_left)
                return result
            except exceptions as err:
                logging.info(f'Backoff: caught exception {err}')
                if total_sleep_left == 0:
                    logging.info('Backoff: total sleep type is over, reraising..')
                    total_sleep_left = total_sleep_time
                    sleep_time = min(start_sleep_time, total_sleep_left)
                    raise
                logging.info(f'Backoff: will try again after sleep {sleep_time} secs..')
                time.sleep(sleep_time)
                total_sleep_left -= sleep_time
                sleep_time *= factor
                sleep_time = min(sleep_time, border_sleep_time, total_sleep_left)
                return inner(*args, **kwargs)

        return inner

    return func_wrapper
