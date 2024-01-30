import uuid
import datetime
from flask import request
from http import HTTPStatus
from flask_restx import Resource

import mindsdb.interfaces.storage.db as db
from mindsdb.interfaces.storage.db import Predictor
from mindsdb.interfaces.storage.db import FineTuningJobs
from mindsdb.interfaces.model.functions import PredictorRecordNotFound
from mindsdb.interfaces.model.functions import get_predictor_integration

from mindsdb.api.http.utils import http_error
from mindsdb.api.http.namespaces.configs.fine_tuning import ns_conf
from mindsdb.api.executor.controllers.session_controller import SessionController


def add_fine_tuning_job(job_id, model_id, training_file, created_at):
    fine_tuning_job_record = db.FineTuningJobs(
        id=job_id,
        model_id=model_id,
        training_file=training_file,
        created_at=created_at,
    )

    db.session.add(fine_tuning_job_record)
    db.session.commit()

def list_fine_tuning_jobs():
    return db.session.query(FineTuningJobs).all()

def get_fine_tuning_job(job_id):
    return db.session.query(FineTuningJobs).filter_by(id=job_id).first()


@ns_conf.route('/jobs')
class FineTuning(Resource):
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
        hyperparameters = request.json.get('hyperparameters', None)
        suffix = request.json.get('suffix', None)
        validation_file = request.json.get('validation_file', None)

        # initialize session controller
        session = SessionController()

        project_name, model_name = model.split('.', 1)

        try:
            # extract model name, version and record
            name_no_version, version = Predictor.get_name_and_version(model_name)
            try:
                model_record = session.model_controller.get_model_record(name_no_version, version=version, project_name=project_name)
            except PredictorRecordNotFound:
                return http_error(
                    HTTPStatus.NOT_FOUND,
                    'Model not found',
                    f'Model with name {model} not found')
            
            # get the integration handler
            integration_name = get_predictor_integration(model_record).name

            # get handler instance
            base_ml_engine = session.integration_controller.get_handler(integration_name)

            # generate job ID
            job_id = uuid.uuid4().hex
            created_at = datetime.datetime.now()

            # execute fine-tuning job
            fine_tuned_model_record = base_ml_engine.finetune(
                model_name=model_record.name,
                base_model_version=model_record.version,
                project_name=project_name,
                data_integration_ref=model_record.data_integration_ref,
                fetch_data_query=f"SELECT * FROM {training_file}",
            )

            # store job details in DB
            add_fine_tuning_job(job_id, fine_tuned_model_record.id, training_file, created_at)
            
            return {
                'object': 'fine_tuning.job',
                'id': job_id,
                'model': model_name,
                'training_file': training_file,
                'created_at': int(created_at.timestamp())
            }, HTTPStatus.OK
        except Exception as e:
            return str(e), HTTPStatus.INTERNAL_SERVER_ERROR
        
    @ns_conf.doc('list_fine_tuning_jobs')
    def get(self):
        try:
            fine_tuning_jobs = list_fine_tuning_jobs()

            return [job.update({'object': 'fine_tuning.job'}) for job in fine_tuning_jobs], HTTPStatus.OK
        except Exception as e:
            return str(e), HTTPStatus.INTERNAL_SERVER_ERROR
        

@ns_conf.route('/jobs/<job_id>')
class FineTuningJob(Resource):
    @ns_conf.doc('get_fine_tuning_job')
    def get(self, job_id):
        try:
            fine_tuning_job = get_fine_tuning_job(job_id)

            return fine_tuning_job.as_dict(), HTTPStatus.OK
        except Exception as e:
            return str(e), HTTPStatus.INTERNAL_SERVER_ERROR