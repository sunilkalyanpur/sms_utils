"""
Date time Utility functions
"""

import datetime
import time


def epoch_to_formatted_timestamp(epoch, format="%Y-%m-%dT%H:%M:%S"):
    """
    :param epoch:
    :param format:
    :return:
    """
    try:
        return time.strftime(format, time.gmtime(epoch))
    except ValueError as error:
        raise error


def get_timedelta_from_time_string(time_string):
    try:
        hours, minutes, seconds = map(float, time_string.split(':'))
        return datetime.timedelta(hours=hours, minutes=minutes, seconds=seconds)
    except Exception as error:
        raise error

