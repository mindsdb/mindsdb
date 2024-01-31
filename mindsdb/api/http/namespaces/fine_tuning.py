import datetime
from flask import request
from http import HTTPStatus
from typing import Text, Dict
from flask_restx import Resource

import mindsdb.interfaces.storage.db as db
from mindsdb.interfaces.storage.db import Predictor
from mindsdb.interfaces.storage.db import FineTuningJobs
from mindsdb.interfaces.model.functions import PredictorRecordNotFound
from mindsdb.interfaces.model.functions import get_predictor_integration

from mindsdb.api.http.utils import http_error
from mindsdb.api.http.namespaces.configs.fine_tuning import ns_conf
from mindsdb.api.executor.controllers.session_controller import SessionController


def add_fine_tuning_job(model_id: int, training_file: Text, validation_file: Text, created_at: datetime.datetime) -> int:
    """
    Add a new fine-tuning job to the database.

    Parameters
    ----------
    model_id: int
        ID of the model (Predictor) to be fine-tuned.
    training_file: str
        Name of the training file.
    validation_file: str
        Name of the validation file.
    created_at: datetime.datetime
        Datetime when the fine-tuning job was created.

    Returns
    -------
    int
        ID of the fine-tuning job record created in the database.
    """

    # create fine-tuning job record
    fine_tuning_job_record = db.FineTuningJobs(
        model_id=model_id,
        training_file=training_file,
        validation_file=validation_file,
        created_at=created_at,
    )

    # add fine-tuning job record to DB
    db.session.add(fine_tuning_job_record)
    db.session.commit()

    return fine_tuning_job_record.id


def list_fine_tuning_jobs(after: int, limit: int) -> Dict:
    """
    List fine-tuning jobs.

    Parameters
    ----------
    after: int
        ID of the last fine-tuning job returned in the previous request.
    limit: int
        Maximum number of fine-tuning jobs to return.

    Returns
    -------
    Dict
        Dictionary with the list of fine-tuning jobs and a flag indicating if there are more jobs to return.
    """

    # get fine-tuning job records
    fine_tuning_job_records = (
        db.session.query(FineTuningJobs, Predictor)
        .join(Predictor, Predictor.id == FineTuningJobs.model_id)
        .filter(FineTuningJobs.id > after)
        .limit(limit + 1)
        .all()
    )

    # check if there are more records
    has_more = len(fine_tuning_job_records) > limit

    # if there are more records, remove the last one
    if has_more:
        fine_tuning_job_records.pop()

    # iterate over fine-tuning job records and parse them
    fine_tuning_jobs = []
    for fine_tuning_job_record, predictor_record in fine_tuning_job_records:
        fine_tuning_job = parse_fine_tuning_job_data(fine_tuning_job_record, predictor_record)

        fine_tuning_jobs.append(fine_tuning_job)

    return {
        'object': 'list',
        'data': fine_tuning_jobs,
        'has_more': has_more,
    }


def get_fine_tuning_job(job_id: int) -> Dict:
    """
    Get a fine-tuning job.

    Parameters
    ----------
    job_id: int
        ID of the fine-tuning job.

    Returns
    -------
    Dict
        Dictionary with the fine-tuning job.
    """

    fine_tuning_job_record, predictor_record = (
        db.session.query(FineTuningJobs, Predictor)
        .join(Predictor, Predictor.id == FineTuningJobs.model_id)
        .filter(FineTuningJobs.id == job_id)
        .first()
    )

    # parse the fine-tuning job record
    fine_tuning_job = parse_fine_tuning_job_data(fine_tuning_job_record, predictor_record)

    return fine_tuning_job


def cancel_fine_tuning_job(job_id):
    pass


def parse_fine_tuning_job_data(fine_tuning_job_record, predictor_record):
    """
    Parse fine-tuning job data and add more information for the response.

    Parameters
    ----------
    fine_tuning_job_record: mindsdb.interfaces.storage.db.FineTuningJobs
        Fine-tuning job record.
    predictor_record: mindsdb.interfaces.storage.db.Predictor

    Returns
    -------
    Dict
        Dictionary with the fine-tuning job.
    """

    # convert fine-tuning job record to dictionary
    fine_tuning_job = fine_tuning_job_record.as_dict()

    # add object type
    fine_tuning_job['object'] = 'fine_tuning.job'

    # remove model_id and add model
    fine_tuning_job.pop('model_id')
    fine_tuning_job['model'] = f"{predictor_record.name}.{predictor_record.version}"

    # add organization ID
    fine_tuning_job['organization_id'] = predictor_record.company_id

    # convert created_at to timestamp
    fine_tuning_job['created_at'] = int(fine_tuning_job['created_at'].timestamp())

    # update status of fine-tuning job based on model status
    if predictor_record.status == 'generating':
        fine_tuning_job['status'] = 'running'
        fine_tuning_job['finished_at'] = None
        fine_tuning_job['fine_tuned_model'] = None

    elif predictor_record.status == 'complete':
        fine_tuning_job['status'] = 'succeeded'
        fine_tuning_job['finished_at'] = int(predictor_record.updated_at.timestamp())
        fine_tuning_job['fine_tuned_model'] = f"{predictor_record.name}.{predictor_record.version + 1}"

    elif predictor_record.status == 'error':
        fine_tuning_job['status'] = 'failed'
        fine_tuning_job['finished_at'] = int(predictor_record.updated_at.timestamp())
        fine_tuning_job['fine_tuned_model'] = None

    # TODO: add support for other statuses

    return fine_tuning_job


@ns_conf.route('/jobs')
class FineTuningJobsCreateAndList(Resource):
    # TODO: table should not be created here
    def create_table_if_not_exists(self):
        FineTuningJobs.metadata.create_all(db.session.get_bind(), checkfirst=True)

    @ns_conf.doc('create_fine_tuning_job')
    def post(self):
        # TODO: table should not be created here
        self.create_table_if_not_exists()

        # extract parameters from request
        try:
            model = request.json['model']
            training_file = request.json['training_file']
        except KeyError:
            return 'The model and training_file parameters are required', HTTPStatus.BAD_REQUEST

        # TODO: add support for hyperparameters, suffix and validation_file
        # hyperparameters = request.json.get('hyperparameters', None)
        # suffix = request.json.get('suffix', None)
        validation_file = request.json.get('validation_file', None)

        # initialize session controller
        session = SessionController()

        project_name, model_name = model.split('.', 1)

        try:
            # extract model name, version and record
            name_no_version, version = Predictor.get_name_and_version(model_name)
            try:
                predictor_record = session.model_controller.get_model_record(name_no_version, version=version, project_name=project_name)
            except PredictorRecordNotFound:
                return http_error(
                    HTTPStatus.NOT_FOUND,
                    'Model not found',
                    f'Model with name {model} not found')

            # get the integration handler
            integration_name = get_predictor_integration(predictor_record).name

            # get handler instance
            base_ml_engine = session.integration_controller.get_handler(integration_name)

            created_at = datetime.datetime.now()

            # execute fine-tuning job
            fine_tuned_predictor_record = base_ml_engine.finetune(
                model_name=predictor_record.name,
                base_model_version=predictor_record.version,
                project_name=project_name,
                data_integration_ref=predictor_record.data_integration_ref,
                fetch_data_query=f"SELECT * FROM {training_file}",
            )

            # store job details in DB
            job_id = add_fine_tuning_job(fine_tuned_predictor_record.id, training_file, validation_file, created_at)

            # TODO: add status and result_files (?) to response
            return {
                'object': 'fine_tuning.job',
                'id': job_id,
                'model': model_name,
                'created_at': int(created_at.timestamp()),
                'fine_tuned_model': None,
                'organization_id': predictor_record.company_id,
                'validation_file': validation_file,
                'training_file': training_file,
            }, HTTPStatus.OK
        except Exception as e:
            return str(e), HTTPStatus.INTERNAL_SERVER_ERROR

    @ns_conf.doc('list_fine_tuning_jobs')
    def get(self):
        # extract parameters from request
        after = request.args.get('after', 0)
        limit = int(request.args.get('limit', 20))

        try:
            fine_tuning_jobs = list_fine_tuning_jobs(after, limit)

            return fine_tuning_jobs, HTTPStatus.OK
        except Exception as e:
            return str(e), HTTPStatus.INTERNAL_SERVER_ERROR


@ns_conf.route('/jobs/<job_id>')
class FineTuningJobGet(Resource):
    @ns_conf.doc('get_fine_tuning_job')
    def get(self, job_id):
        try:
            fine_tuning_job = get_fine_tuning_job(job_id)

            return fine_tuning_job, HTTPStatus.OK
        except Exception as e:
            return str(e), HTTPStatus.INTERNAL_SERVER_ERROR


@ns_conf.route('/jobs/<job_id>/cancel')
class FineTuningJobCancel(Resource):
    @ns_conf.doc('cancel_fine_tuning_job')
    def post(self, job_id):
        # TODO: cancelling query execution is not supported yet, it is tracked here: https://github.com/mindsdb/mindsdb/issues/5728
        pass
