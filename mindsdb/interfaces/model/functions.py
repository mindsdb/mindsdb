from sqlalchemy import null

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


def get_integration_record(company_id: int, name: str):
    record = (
        db.session.query(db.Integration)
        .filter_by(company_id=company_id, name=name)
        .first()
    )
    return record


def get_predictor_integration(record: db.Predictor) -> db.Integration:
    integration_record = (
        db.session.query(db.Integration)
        .filer_by(id=record.integration_id).first()
    )
    return integration_record


def get_model_records(company_id: int, active: bool = True, deleted_at=null(),
                      ml_handler_name: str = None, **kwargs):
    if company_id is None:
        kwargs['company_id'] = null()
    else:
        kwargs['company_id'] = company_id
    kwargs['deleted_at'] = deleted_at
    if active is not None:
        kwargs['active'] = active

    if ml_handler_name is not None:
        ml_handler_record = get_integration_record(
            company_id=company_id,
            name=ml_handler_name
        )
        if ml_handler_record is None:
            raise Exception(f'unknown ml handler: {ml_handler_name}')
        kwargs['integration_id'] = ml_handler_record.id

    return (
        db.session.query(db.Predictor, db.Integration.name)
        .filter_by(**kwargs)
        .all()
    )


def get_model_record(company_id: int, except_absent=False, ml_handler_name: str = None,
                     active: bool = True, deleted_at=null(), **kwargs):
    if company_id is None:
        kwargs['company_id'] = null()
    else:
        kwargs['company_id'] = company_id
    kwargs['deleted_at'] = deleted_at
    if active is not None:
        kwargs['active'] = active

    if ml_handler_name is not None:
        ml_handler_record = get_integration_record(
            company_id=company_id,
            name=ml_handler_name
        )
        if ml_handler_record is None:
            raise Exception(f'unknown ml handler: {ml_handler_name}')
        kwargs['integration_id'] = ml_handler_record.id

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
