import functools
import logging
from aws_xray_sdk.core import xray_recorder

log = logging.getLogger(__name__)


def trace_event_source(handler):


    def handler_wrapper(event, context, *args, **kwargs):

        @xray_recorder.capture()
        def inner_handler():
            subsegment = xray_recorder.current_subsegment()

            if subsegment:
                subsegment.name = 'Lambda Handler Context'
                subsegment.namespace = 'AWS'
                subsegment.aws['event_source'] = get_event_source(event)

            return handler(event, context, *args, **kwargs)

        return inner_handler()

    return handler_wrapper


def get_in(xs, keys, default=None):

    def f(ys, k):
        try:
            return ys[k]
        except (KeyError, IndexError, TypeError):
            return default

    return functools.reduce(f, keys, xs)

import json
def get_event_source(event):
    try:
        api_id = get_in(event, ['requestContext', 'apiId'])
        if api_id:
            account = get_in(event, ['requestContext', 'accountId'])
            region = get_in(event, ['multiValueHeaders', 'Host', 0], default='a.b.us-east-1.amazonaws.com').split('.')[2]
            method = get_in(event, ['requestContext', 'httpMethod'])
            path = get_in(event, ['requestContext', 'path'])
            return f'arn:aws:execute-api:{region}:{account}:{api_id}/*/{method}/{path}'

        # Yes AWS sometimes returns either eventSource or EventSource
        event_source = str(get_in(event, ['Records', 0, 'eventSource'])) or str(get_in(event, ['Records', 0, 'EventSource']))
        if 'sns' in event_source:
            return get_in(event, ['Records', 0, 'Sns', 'TopicArn'])
        elif 'dynamodb' in event_source or 'kinesis' in event_source:
            return get_in(event, ['Records', 0, 'eventSourceARN'])
        elif "s3" in event_source:
            return get_in(event, ['Records', 0, 's3', 'bucket', 'arn'])
        else:
            log.warning(f'Could not get detailed client details from {event_source}')
    except (AttributeError, KeyError, IndexError):
        log.error('Error getting client details', exc_info=True)

    return 'Unknown'
