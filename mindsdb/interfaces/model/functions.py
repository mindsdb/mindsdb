from typing import Optional

from sqlalchemy import null, func

import mindsdb.interfaces.storage.db as db
from mindsdb.utilities.context import context as ctx


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


def get_integration_record(name: str) -> db.Integration:
    company_id = ctx.company_id
    if company_id is None:
        company_id = null()

    record = (
        db.session.query(db.Integration)
        .filter_by(company_id=company_id, name=name)
        .first()
    )
    return record


def get_project_record(name: str) -> db.Project:
    company_id = ctx.company_id
    if company_id is None:
        company_id = null()

    project_record = (
        db.session.query(db.Project)
        .filter(
            (func.lower(db.Project.name) == name)
            & (db.Project.company_id == company_id)
            & (db.Project.deleted_at == null())
        ).first()
    )
    return project_record


def get_predictor_integration(record: db.Predictor) -> db.Integration:
    integration_record = (
        db.session.query(db.Integration)
        .filter_by(id=record.integration_id).first()
    )
    return integration_record


def get_predictor_project(record: db.Predictor) -> db.Project:
    project_record = (
        db.session.query(db.Project)
        .filter_by(id=record.project_id).first()
    )
    return project_record


def get_model_records(integration_id=None, active=True, deleted_at=null(),
                      project_name: Optional[str] = None, ml_handler_name: Optional[str] = None, **kwargs):
    kwargs['company_id'] = ctx.company_id
    if kwargs['company_id'] is None:
        kwargs['company_id'] = null()

    if deleted_at is not None:
        kwargs['deleted_at'] = deleted_at
    if active is not None:
        kwargs['active'] = active

    if project_name is not None:
        project_record = get_project_record(name=project_name)
        if project_record is None:
            return []
        kwargs['project_id'] = project_record.id

    if ml_handler_name is not None:
        ml_handler_record = get_integration_record(
            name=ml_handler_name
        )
        if ml_handler_record is None:
            # raise Exception(f'unknown ml handler: {ml_handler_name}')
            return []
        kwargs['integration_id'] = ml_handler_record.id

    if integration_id is not None:
        kwargs['integration_id'] = integration_id

    return (
        db.session.query(db.Predictor)
        .filter_by(**kwargs)
        .all()
    )


def get_model_record(except_absent=False, ml_handler_name: Optional[str] = None,
                     project_name: Optional[str] = None, active: bool = True,
                     deleted_at=null(), version: Optional[int] = None, **kwargs):
    kwargs['company_id'] = ctx.company_id
    if kwargs['company_id'] is None:
        kwargs['company_id'] = null()

    kwargs['deleted_at'] = deleted_at
    if active is not None:
        # not use active if version was chosen
        if version is not None:
            kwargs['version'] = version
        else:
            kwargs['active'] = active

    if project_name is not None:
        project_record = get_project_record(name=project_name)
        if project_record is None:
            return []
        kwargs['project_id'] = project_record.id

    if ml_handler_name is not None:
        ml_handler_record = get_integration_record(
            name=ml_handler_name
        )
        if ml_handler_record is None:
            # raise Exception(f'unknown ml handler: {ml_handler_name}')
            return []
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
