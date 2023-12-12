"""
Utility.py contains utility to deal with data, which contains
    1. mini class
"""
import datetime

def ParseElapsedTime(s_time, e_time):
    elapse = (e_time - s_time).seconds
    m, s = divmod(elapse, 60)
    h, m = divmod(m, 60)
    return h, m, s


class TimerWatch:
    def __init__(self, info: str):
        self.info = info
        self.start_time = datetime.datetime.now()
        self.end_time = datetime.datetime.now()

    def __enter__(self):
        self.start_time = datetime.datetime.now()
        return self

    def __exit__(self, exc_type, exc_value, exc_trace):
        self.end_time = datetime.datetime.now()

        h, m, s = ParseElapsedTime(self.start_time, self.end_time)
        print(f"{self.info} ({h:02d}:{m:02d}:{s:02d})")
