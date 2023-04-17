from datetime import datetime, timedelta
from typing import List

def gen_date_range(dt_start: datetime, dt_end: datetime,
                   dt_delta: timedelta, subtract_ed: timedelta = timedelta(days=1),
                   format: str = None) -> List[dict]:
    """
    Generates date range from start up to end date in intervals of dt_delta.
    Subtracts subtract_ed from each end_date.
    :param dt_start: start date.
    :param dt_end: end date.
    :param dt_delta: interval between each start and end date.
    :param subtract_ed: interval to subtract from each end date.
    :param format: string with the time format. If None (default) returns the datetime object (ex: "%Y-%m-%d %H:%M:%S").
    :return: list of dictionary with the start and end dates.
    """

    sd = dt_start
    ed = dt_start + dt_delta

    result = []

    # create date range
    while sd < dt_end:

        if ed > dt_end:
            ed = dt_end

        result.append({'dt_start': sd, 'dt_end': ed - subtract_ed})

        sd += dt_delta
        ed += dt_delta

    if format is None:
        return result

    # Convert to string if format is not None.
    for i in range(len(result)):

        result[i]['dt_start'] = result[i]['dt_start'].strftime(format)
        result[i]['dt_end'] = result[i]['dt_end'].strftime(format)

    return result


if __name__ == '__main__':

    dt_start = datetime(2022, 1, 1)
    dt_end = datetime(2022, 12, 31)
    dt_delta = timedelta(days=15)
    subtract_ed = timedelta(days=1)

    date_range = gen_date_range(dt_start, dt_end, dt_delta, subtract_ed=subtract_ed, format='%d/%m/%Y')
    for d in date_range:
        print(d)