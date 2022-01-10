# helper.py
#
# Some general helper functions
#
# written by Chris Seal <c.seal@auckland.ac.nz>
#
# Last updated: 29 May 2020
#

from datetime import datetime as dt
import dateutil.relativedelta as ddelta
import re


def lowercase(obj):
    """ Make dictionary lowercase """
    if isinstance(obj, dict):
        return {k.lower(): lowercase(v) for k, v in obj.items()}
    elif isinstance(obj, (list, set, tuple)):
        t = type(obj)
        return t(lowercase(o) for o in obj)
    elif isinstance(obj, str):
        return obj.lower()
    else:
        return obj


def check_dictionary(dictionary,
                     required_keys):
    # TODO Refactor to remove this since sanity.py does the same thing
    '''Carry out basic sanity tests on a dictionary

    Inputs:
    =================================
    dictionary: The dictionary to check
    required_keys: A list of required keys for the dictionary

    Returns:
    =================================
    True and an empty list if all the keys are found
    False and a list of missing keys if some are missing
    '''
    lost_keys = []
    for key in required_keys:
        if key not in dictionary.keys():
            lost_keys.append(key)
    if lost_keys == []:
        return (True, [])
    else:
        return (False, lost_keys)


def most_probable_date(test_string):
    '''Function to try and parse a potential date string through a range of different
    potential date string formats. For each match that is made, determine the relative difference
    between now and the potential date. If the date is in the future then it is not correct so dump it.
    If there are more than one possible dates in the past, then find the one that is closest to the
    current date and pick this one.'''
    now = dt.now()
    dateformats = ['%Y-%m-%d',
                   '%d-%m-%Y',
                   '%d-%m-%y',
                   '%y-%m-%d',
                   '%Y-%b-%d',
                   '%d-%b-%Y',
                   '%y-%b-%d',
                   '%d-%b-%y']
    delimiters = ['',
                  '-',
                  '_',
                  ' ']
    dates = []
    for delim in delimiters:
        for dateformat in dateformats:
            frmt = dateformat.replace('-', delim)
            try:
                dir_date = dt.strptime(test_string, frmt)
            except ValueError:
                continue
            else:
                dates.append(dir_date)
    if dates == []:
        return None
    else:
        timedelta = []
        for date in dates:
            timedelta.append(ddelta.relativedelta(date, now))
        most_prob_index = min_abs_delta(timedelta)
    return dates[most_prob_index]


def research_code_from_string(test_string):
    reg_exps = [r'res[a-z]{3}20[0-9]{6,}',
                r'rvm[a-z][0-9]{5,}']
    for reg_exp in reg_exps:
        data = test_string.split('-')
        for rescode in data:
            if re.match(reg_exp, rescode):
                return rescode
        data = test_string.split('_')
        for rescode in data:
            if re.match(reg_exp, rescode):
                return rescode
    return None


def upi_from_string(test_string):
    data = test_string.split('-')
    for part in data:
        if re.match(r'[a-z]{4}[0-9]{3}', part):
            return part
    data = test_string.split('_')
    for part in data:
        if re.match(r'[a-z]{4}[0-9]{3}', part):
            return part
    return None


def min_abs_delta(delta_list):
    day_list = []
    for delta in delta_list:
        if not delta.days:
            delta.days = 0
        if delta.years:
            if delta.months:
                months = delta.months+12*delta.years
                day_list.append(30*months + delta.days)
            else:
                months = 12*delta.years
                day_list.append(30*months + delta.days)
        else:
            if delta.months:
                months = delta.months
                day_list.append(30*months + delta.days)
            else:
                day_list.append(delta.days)
    min_delta = -1e6
    if len(day_list) == 1:
        return 0
    index = -1
    for ind in range(len(day_list)):
        day = day_list[ind]
        if day <= 0 and day > min_delta:
            index = ind
            min_delta = day
    return index
