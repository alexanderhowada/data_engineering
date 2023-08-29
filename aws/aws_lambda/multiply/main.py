import json
from functools import reduce

import numpy as np

def main(event, context):

    if len(event['array']) != 0:
        s = int(np.sum(event['array']))
        m = reduce(lambda x, y: x*y, event['array'])
    else:
        s = None
        m = None

    r = {
        's': s, 'm': m
    }

    return {
        'statusCode': 200,
        'body': json.dumps(r)
    }