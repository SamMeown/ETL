from functools import wraps
import time


class BackoffTimeoutException(Exception):
    """Ошибка кидается, когда полное время, выделенное на повторы истекло"""
    pass


def backoff(start_sleep_time=0.1, factor=2, border_sleep_time=10, total_sleep_time=30):
    """
    Функция для повторного выполнения функции через некоторое время, если возникла ошибка. Использует наивный
    экспоненциальный рост времени повтора (factor) до граничного времени ожидания (border_sleep_time)

    Формула:
        t = start_sleep_time * 2^(n) if t < border_sleep_time
        t = border_sleep_time if t >= border_sleep_time
    :param start_sleep_time: начальное время повтора
    :param factor: во сколько раз нужно увеличить время ожидания
    :param border_sleep_time: граничное время ожидания
    :param total_sleep_time: максимальное время, выделенное на все повторы
    :return: результат выполнения функции
    """

    def func_wrapper(func):
        sleep_time = 0
        total_sleep_left = total_sleep_time

        @wraps(func)
        def inner(*args, **kwargs):
            nonlocal sleep_time, total_sleep_left
            if total_sleep_left == 0:
                total_sleep_left = total_sleep_time
                raise BackoffTimeoutException
            if sleep_time:
                time.sleep(sleep_time)
                total_sleep_left -= sleep_time
                sleep_time *= factor
                sleep_time = min(sleep_time, border_sleep_time, total_sleep_left)
            else:
                sleep_time = min(start_sleep_time, total_sleep_left)
            result = func(*args, **kwargs)
            sleep_time = 0
            total_sleep_left = total_sleep_time
            return result

        return inner

    return func_wrapper
