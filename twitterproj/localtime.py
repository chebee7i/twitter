"""
Functions for counting localtime statistics.

"""
import datetime
import pytz

from .helpers import connect

TIMEZONES = [('Pacific',  pytz.timezone('America/Los_Angeles')),
             ('Mountain', pytz.timezone('America/Denver')),
             ('Central',  pytz.timezone('America/Chicago')),
             ('Eastern',  pytz.timezone('America/New_York'))]

def get_ustz(localdt, timezone, use_noon=False):
    """
    Returns the timezone associated to a local datetime and an IANA timezone.

    There are two common timezone conventions. One is the Olson/IANA and
    the other is the Microsoft convention. For example, the closest IANA
    timezone for Boston, MA is America/New_York. More commonly, this is
    known as Eastern time zone. The goal of this function is to return the
    common name for a timezone in the contiguous US.

    Note that Arizona has its own IANA timezone and does not observe daylight
    savings. So depending on the time of year, the offset for Arizona will
    correspond to either Pacific or Mountain time.

    Parameters
    ----------
    localdt : datetime
        The local datetime instance.
    timezone : str
        The IANA timezone associated with `localdt`. This should be a timezone
        for the contiguous US.
    use_noon : bool
        If `True`, ignore the time for the incoming datetime and use noon
        instead. This is nice for quick checks, but undesirable when accurate
        timezone identification is needed late at night or early morning

    Returns
    ------
    tz : str
        The common name for the timezone. This will be one of Pacific, Mountain,
        Central, or Eastern.

    """
    # Use noon to guarantee that we have the same day in each timezone.
    localdt = datetime.datetime(localdt.year, localdt.month, localdt.day, 12)

    timezone = pytz.timezone(timezone)
    dt = timezone.localize(localdt)

    for tz, tz_ref in TIMEZONES:
        dt_new = dt.astimezone(tz_ref)
        if dt_new.utcoffset() == dt.utcoffset():
            return tz

