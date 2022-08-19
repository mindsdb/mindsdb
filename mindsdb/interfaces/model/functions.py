import mindsdb.interfaces.storage.db as db


class PredictorRecordNotFound(Exception):
    def __init__(self, **kwargs):
        name = kwargs.get('name') or '-'
        predictor_id = kwargs.get('id') or '-'
        super().__init__(
            f"Predictor not found: name='{name}' id='{predictor_id}'"
        )


class MultiplePredictorRecordsFound(Exception):
    def __init__(self, **kwargs):
        name = kwargs.get('name') or '-'
        predictor_id = kwargs.get('id') or '-'
        super().__init__(
            f"Found multiple predictor with: name='{name}' id='{predictor_id}'"
        )


def get_model_records(company_id: int, active: bool = True, **kwargs):
    kwargs['company_id'] = company_id
    kwargs['active'] = active
    return (
        db.session.query(db.Predictor)
        .filter_by(**kwargs)
        .all()
    )


def get_model_record(company_id: int, except_absent=False,
                     active: bool = True, **kwargs):
    kwargs['company_id'] = company_id
    kwargs['active'] = active

    records = (
        db.session.query(db.Predictor)
        .filter_by(**kwargs)
        .all()
    )
    if len(records) > 1:
        raise MultiplePredictorRecordsFound(**kwargs)
    if len(records) == 0:
        if except_absent is True:
            raise PredictorRecordNotFound(**kwargs)
        else:
            return None
    return records[0]
