from functools import wraps
import time


def backoff(start_sleep_time=0.1, factor=2, border_sleep_time=10):
    """
    Функция для повторного выполнения функции через некоторое время, если возникла ошибка. Использует наивный
    экспоненциальный рост времени повтора (factor) до граничного времени ожидания (border_sleep_time)

    Формула:
        t = start_sleep_time * 2^(n) if t < border_sleep_time
        t = border_sleep_time if t >= border_sleep_time
    :param start_sleep_time: начальное время повтора
    :param factor: во сколько раз нужно увеличить время ожидания
    :param border_sleep_time: граничное время ожидания
    :return: результат выполнения функции
    """

    def func_wrapper(func):
        sleep_time = 0

        @wraps(func)
        def inner(*args, **kwargs):
            nonlocal sleep_time
            if sleep_time:
                time.sleep(sleep_time)
                sleep_time *= factor
                sleep_time = min(sleep_time, border_sleep_time)
            else:
                sleep_time = start_sleep_time
            result = func(*args, **kwargs)
            sleep_time = 0
            return result

        return inner

    return func_wrapper