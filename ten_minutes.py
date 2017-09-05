#!/bin/python

import sys
import json


def tenminute(s):
    return s, divmod(int(s), 600000)[0]

for line in sys.stdin:
    n_line = line.strip().split('\t')
    request_timestamp = n_line[0]
    insert_time = n_line[1]
    action = n_line[2]
    remote_host = n_line[3]
    response_timestamp = n_line[4]
    recv_time = n_line[5]
    request = n_line[6]
    response = n_line[7]
    properties = n_line[8]
    p_day = n_line[9]
    p_hour = n_line[10]
    ten_minute = tenminute(json.loads(properties)['charge_timestamp'])[1]

    print('%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s' %
          (request_timestamp, insert_time, action, remote_host, response_timestamp, recv_time, request, response, properties, p_day, p_hour, ten_minute)
          )
