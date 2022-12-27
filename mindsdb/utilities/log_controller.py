from mindsdb.interfaces.storage import db
from mindsdb.utilities.context import context as ctx


def fmt_log_record(log_record):
    return {
        'log_from': 'mindsdb',
        'level': log_record.log_type,
        'context': 'unkown',
        'text': log_record.payload,
        'created_at': str(log_record.created_at).split('.')[0]
    }


def get_logs(min_timestamp, max_timestamp, context, level, log_from, limit):
    logs = db.session.query(db.Log).filter(
        db.Log.company_id == ctx.company_id,
        db.Log.created_at > min_timestamp
    )

    if max_timestamp is not None:
        logs = logs.filter(db.Log.created_at < max_timestamp)

    if context is not None:
        # e.g. datasource/predictor and assoicated id
        pass

    if level is not None:
        logs = logs.filter(db.Log.log_type == level)

    if log_from is not None:
        # mindsdb/native/lightwood/all
        pass

    if limit is not None:
        logs = logs.limit(limit)

    logs = [fmt_log_record(x) for x in logs]
    return logs
