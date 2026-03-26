from http import HTTPStatus

from flask import request, current_app as ca
from flask_restx import Resource

from mindsdb.api.http.namespaces.configs.projects import ns_conf
from mindsdb.api.http.utils import http_error
from mindsdb.metrics.metrics import api_endpoint_metrics

from mindsdb.interfaces.jobs.jobs_controller import parse_job_date


@ns_conf.route("/<project_name>/jobs")
class JobsResource(Resource):
    @ns_conf.doc("list_jobs")
    @api_endpoint_metrics("GET", "/jobs")
    def get(self, project_name):
        """List all jobs in a project"""
        return ca.jobs_controller.get_list(project_name)

    @ns_conf.doc("create_job")
    @api_endpoint_metrics("POST", "/jobs")
    def post(self, project_name):
        """Create a job in a project"""

        # Check for required parameters.
        if "job" not in request.json:
            return http_error(HTTPStatus.BAD_REQUEST, "Missing parameter", 'Must provide "job" parameter in POST body')

        job = request.json["job"]

        name = job.pop("name")
        if job["start_at"] is not None:
            job["start_at"] = parse_job_date(job["start_at"])
        if job["end_at"] is not None:
            job["end_at"] = parse_job_date(job["end_at"])

        create_job_name = ca.jobs_controller.add(name, project_name, **job)

        return ca.jobs_controller.get(create_job_name, project_name)


@ns_conf.route("/<project_name>/jobs/<job_name>")
@ns_conf.param("project_name", "Name of the project")
@ns_conf.param("job_name", "Name of the job")
class JobResource(Resource):
    @ns_conf.doc("get_job")
    @api_endpoint_metrics("GET", "/jobs/job")
    def get(self, project_name, job_name):
        """Gets a job by name"""
        job_info = ca.jobs_controller.get(job_name, project_name)
        if job_info is not None:
            return job_info

        return http_error(HTTPStatus.NOT_FOUND, "Job not found", f"Job with name {job_name} does not exist")

    @ns_conf.doc("delete_job")
    @api_endpoint_metrics("DELETE", "/jobs/job")
    def delete(self, project_name, job_name):
        """Deletes a job by name"""
        ca.jobs_controller.delete(job_name, project_name)

        return "", HTTPStatus.NO_CONTENT


@ns_conf.route("/<project_name>/jobs/<job_name>/history")
@ns_conf.param("project_name", "Name of the project")
@ns_conf.param("job_name", "Name of the job")
class JobsHistory(Resource):
    @ns_conf.doc("job_history")
    @api_endpoint_metrics("GET", "/jobs/job/history")
    def get(self, project_name, job_name):
        """Get history of job calls"""
        if ca.jobs_controller.get(job_name, project_name) is None:
            return http_error(HTTPStatus.NOT_FOUND, "Job not found", f"Job with name {job_name} does not exist")

        return ca.jobs_controller.get_history(job_name, project_name)
