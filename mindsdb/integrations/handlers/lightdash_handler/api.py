import requests
from urllib.parse import urljoin


class Lightdash:

    def __init__(self, url: str, api_key: str) -> None:
        self.base_url = urljoin(url, "/api/v1/")
        self.api_key = api_key

    def _request(self, method: str, relative_endpoint: str, data=None):
        kwargs = {
            "method": method,
            "url": urljoin(self.base_url, relative_endpoint),
            "headers": {
                "Authorization": "ApiKey " + self.api_key,
            }
        }
        if data is not None:
            kwargs["data"] = data
        return requests.request(**kwargs)

    def is_connected(self) -> bool:
        if self.get_user() is not None:
            return True
        return False

    def get_user(self):
        """
        Get user's details
        Return format:
            {'userUuid': '831b6c26-bdc2-4a56-9818-fd8ebaa406ac',
              'email': 'test@test.com',
              'firstName': 'Test',
              'lastName': 'User',
              'organizationUuid': 'd00805a0-b0b4-400d-a136-66f620493f11',
              'organizationName': 'testing-comp',
              'organizationCreatedAt': '2023-10-20T17:46:10.005Z',
              'isTrackingAnonymized': False,
              'isMarketingOptedIn': True,
              'isSetupComplete': True,
              'role': 'admin',
              'isActive': True,
              'abilityRules': [{'action': 'view',
                'subject': 'OrganizationMemberProfile',
                'conditions': {'organizationUuid': 'd00805a0-b0b4-400d-a136-66f620493f11'}},
               {'action': ...
        """
        resp = self._request("get", "user")
        if resp.ok:
            return resp.json()["results"]
        return {}

    def get_org(self):
        """
        Get user's organization details
        Return format:
            {'organizationUuid': 'd00805a0-b0b4-400d-a136-66f620493f11',
              'name': 'testing-comp',
              'defaultProjectUuid': 'string',
              'chartColors': ['string'],
              'needsProject': False}
        """
        resp = self._request("get", "org")
        if resp.ok:
            return resp.json()["results"]
        return {}

    def get_projects(self):
        """
        Get user organization's pojects' details
        Return format:
            [{'name': 'Jaffle shop',
              'projectUuid': '95dfda3b-02e2-4708-a014-5967966020f3',
              'type': 'DEFAULT'}]
        """
        resp = self._request("get", "org/projects")
        if resp.ok:
            return resp.json()["results"]
        return []

    def get_org_members(self):
        """
        Get all the members in user's organization
        Return format:
            [{'userUuid': '831b6c26-bdc2-4a56-9818-fd8ebaa406ac',
              'firstName': 'TestName',
              'lastName': 'TestLastName',
              'email': 'test@test.com',
              'organizationUuid': 'd00805a0-b0b4-400d-a136-66f620493f11',
              'role': 'admin',
              'isActive': True,
              'isInviteExpired': False}]
        """
        resp = self._request("get", "org/users")
        if resp.ok:
            return resp.json()["results"]
        return []

    def get_project(self, project_uuid: str):
        """
        Get details of a project in user's organization
        Return format:
            { "dbtVersion": "v1.4",
              "copiedFromProjectUuid": "string",
              "pinnedListUuid": "string",
              "warehouseConnection": {
                "role": "string",
                "type": "snowflake",
                "account": "string",
                "database": "string",
                "warehouse": "string",
                "schema": "string",
                "threads": 0,
                "clientSessionKeepAlive": true,
                "queryTag": "string",
                "accessUrl": "string",
                "startOfWeek": 0 },
              "dbtConnection": {
                "type": "dbt",
                "target": "string",
                "environment": [ {
                    "value": "string",
                    "key": "string" } ],
                "profiles_dir": "string",
                "project_dir": "string" },
              "type": "DEFAULT",
              "name": "string",
              "projectUuid": "string",
              "organizationUuid": "string" }
        """
        resp = self._request("get", f"projects/{project_uuid}")
        if resp.ok:
            return resp.json()["results"]
        return {}

    def get_charts_in_project(self, project_uuid: str):
        """
        List all charts in a project
        Return format:
            [{"name": "string",
              "organizationUuid": "string",
              "uuid": "string",
              "description": "string",
              "projectUuid": "string",
              "spaceUuid": "string",
              "pinnedListUuid": "string",
              "spaceName": "string",
              "dashboardUuid": "string",
              "dashboardName": "string",
              "chartType": "string"}]
        """
        resp = self._request("get", f"projects/{project_uuid}/charts")
        if resp.ok:
            return resp.json()["results"]
        return []

    def get_spaces_in_project(self, project_uuid: str):
        """
        List all spaces in a project
        Return format:
            [{"name": "string",
              "organizationUuid": "string",
              "uuid": "string",
              "projectUuid": "string",
              "pinnedListUuid": "string",
              "pinnedListOrder": 0,
              "isPrivate": true,
              "dashboardCount": 0,
              "chartCount": 0,
              "access": [
                  "string" ]}]
        """
        resp = self._request("get", f"projects/{project_uuid}/spaces")
        if resp.ok:
            return resp.json()["results"]
        return []

    def get_project_access_list(self, project_uuid: str):
        """
        Get access list for a project. This is a list of users that have been explictly granted access to the project. There may be other users that have access to the project via their organization membership
        Return format:
            [{"lastName": "string",
              "firstName": "string",
              "email": "string",
              "role": "viewer",
              "projectUuid": "string",
              "userUuid": "string" }]
        """
        resp = self._request("get", f"projects/{project_uuid}/access")
        if resp.ok:
            return resp.json()["results"]
        return []

    def get_validation_results(self, project_uuid: str):
        """
        Get validation results for a project. This will return the results of the latest validation job
        Return format:
            [{"source": "chart",
              "spaceUuid": "string",
              "projectUuid": "string",
              "errorType": "chart",
              "error": "string",
              "name": "string",
              "createdAt": "2019-08-24T14:15:22Z",
              "validationId": 0,
              "chartName": "string",
              "chartViews": 0,
              "lastUpdatedAt": "2019-08-24T14:15:22Z",
              "lastUpdatedBy": "string",
              "fieldName": "string",
              "chartType": "line",
              "chartUuid": "string"}]
        """
        resp = self._request("get", f"projects/{project_uuid}/validate")
        if resp.ok:
            return resp.json()["results"]
        return []

    def get_space(self, project_uuid: str, space_uuid: str):
        """
        Get details for a space in a project
        Return format:
            { "pinnedListOrder": 0,
              "pinnedListUuid": "string",
              "access": [ {
                  "role": "viewer",
                  "lastName": "string",
                  "firstName": "string",
                  "userUuid": "string" } ],
              "dashboards": [ {
                  "name": "string",
                  "organizationUuid": "string",
                  "uuid": "string",
                  "description": "string",
                  "updatedAt": "2019-08-24T14:15:22Z",
                  "projectUuid": "string",
                  "updatedByUser": {
                    "userUuid": "string",
                    "firstName": "string",
                    "lastName": "string" },
                  "spaceUuid": "string",
                  "views": 0,
                  "firstViewedAt": "2019-08-24T14:15:22Z",
                  "pinnedListUuid": "string",
                  "pinnedListOrder": 0,
                  "validationErrors": [ {
                      "validationId": 0,
                      "createdAt": "2019-08-24T14:15:22Z",
                      "error": "string" } ] } ],
              "projectUuid": "string",
              "queries": [ {
                  "name": "string",
                  "uuid": "string",
                  "description": "string",
                  "updatedAt": "2019-08-24T14:15:22Z",
                  "updatedByUser": {
                    "userUuid": "string",
                    "firstName": "string",
                    "lastName": "string" },
                  "spaceUuid": "string",
                  "pinnedListUuid": "string",
                  "pinnedListOrder": 0,
                  "firstViewedAt": "2019-08-24T14:15:22Z",
                  "views": 0,
                  "validationErrors": [ {
                      "validationId": 0,
                      "createdAt": "2019-08-24T14:15:22Z",
                      "error": "string" } ],
                  "chartType": "line" } ],
              "isPrivate": true,
              "name": "string",
              "uuid": "string",
              "organizationUuid": "string" }
        """
        resp = self._request("get", f"projects/{project_uuid}/spaces/{space_uuid}")
        if resp.ok:
            return resp.json()["results"]
        return {}

    def get_chart_version_history(self, chart_uuid: str):
        """
        Get chart version history from last 30 days
        Return format:
            [{"createdAt": "2019-08-24T14:15:22Z",
              "chartUuid": "string",
              "versionUuid": "string",
              "createdBy": {
                "userUuid": "string",
                "firstName": "string",
                "lastName": "string" }}]
        """
        resp = self._request("get", f"saved/{chart_uuid}/history")
        if resp.ok:
            return resp.json()["results"]["history"]
        return []

    def get_chart(self, chart_uuid: str, version_uuid: str):
        """
        Get chart details
        Return format:
            { "chart": {
                "dashboardName": "string",
                "dashboardUuid": "string",
                "pinnedListOrder": 0,
                "pinnedListUuid": "string",
                "spaceName": "string",
                "spaceUuid": "string",
                "organizationUuid": "string",
                "updatedByUser": {
                  "userUuid": "string",
                  "firstName": "string",
                  "lastName": "string" },
                "updatedAt": "2019-08-24T14:15:22Z",
                "tableConfig": {
                  "columnOrder": [
                    "string" ] },
                "chartConfig": {
                  "config": {
                    "legendPosition": "horizontal",
                    "showLegend": true,
                    "groupSortOverrides": [
                      "string" ],
                    "groupValueOptionOverrides": {},
                    "groupColorOverrides": {},
                    "groupLabelOverrides": {},
                    "showPercentage": true,
                    "showValue": true,
                    "valueLabel": "hidden",
                    "isDonut": true,
                    "metricId": "string",
                    "groupFieldIds": [
                      "string" ] },
                  "type": "pie" },
                "pivotConfig": {
                  "columns": [
                    "string" ] },
                "metricQuery": {
                  "additionalMetrics": [ {
                      "label": "string",
                      "type": "percentile",
                      "description": "string",
                      "sql": "string",
                      "hidden": true,
                      "round": 0,
                      "compact": "thousands",
                      "format": "km",
                      "table": "string",
                      "name": "string",
                      "index": 0,
                      "filters": [ {
                          "values": [
                            null ],
                          "operator": "isNull",
                          "id": "string",
                          "target": {
                            "fieldRef": "string" },
                          "settings": null,
                          "disabled": true } ],
                      "baseDimensionName": "string",
                      "uuid": "string",
                      "percentile": 0 } ],
                  "tableCalculations": [ {
                      "format": {
                        "suffix": "string",
                        "prefix": "string",
                        "compact": "thousands",
                        "currency": "string",
                        "separator": "default",
                        "round": 0,
                        "type": "default" },
                      "sql": "string",
                      "displayName": "string",
                      "name": "string",
                      "index": 0 } ],
                  "limit": 0,
                  "sorts": [ {
                      "descending": true,
                      "fieldId": "string" } ],
                  "filters": {
                    "metrics": {
                      "or": [
                        null ],
                      "id": "string" },
                    "dimensions": {
                      "or": [
                        null ],
                      "id": "string" } },
                  "metrics": [
                    "string" ],
                  "dimensions": [
                    "string" ] },
                "tableName": "string",
                "description": "string",
                "name": "string",
                "projectUuid": "string",
                "uuid": "string" },
              "createdBy": {
                "userUuid": "string",
                "firstName": "string",
                "lastName": "string" },
              "createdAt": "2019-08-24T14:15:22Z",
              "versionUuid": "string",
              "chartUuid": "string" }
        """
        resp = self._request("get", f"saved/{chart_uuid}/version/{version_uuid}")
        if resp.ok:
            return resp.json()["results"]
        return {}

    def get_scheduler_logs(self, project_uuid: str):
        """
        Get scheduled logs
        Return format:
            { "logs": [ {
                  "details": {},
                  "targetType": "email",
                  "target": "string",
                  "status": "scheduled",
                  "createdAt": "2019-08-24T14:15:22Z",
                  "scheduledTime": "2019-08-24T14:15:22Z",
                  "jobGroup": "string",
                  "jobId": "string",
                  "schedulerUuid": "string",
                  "task": "handleScheduledDelivery"
                } ],
              "dashboards": [ {
                  "dashboardUuid": "string",
                  "name": "string" } ],
              "charts": [ {
                  "savedChartUuid": "string",
                  "name": "string" } ],
              "users": [ {
                  "userUuid": "string",
                  "lastName": "string",
                  "firstName": "string" } ],
              "schedulers": [ {
                  "options": {
                    "limit": 0,
                    "formatted": true },
                  "dashboardUuid": null,
                  "savedChartUuid": "string",
                  "cron": "string",
                  "format": "csv",
                  "createdBy": "string",
                  "updatedAt": "2019-08-24T14:15:22Z",
                  "createdAt": "2019-08-24T14:15:22Z",
                  "message": "string",
                  "name": "string",
                  "schedulerUuid": "string",
                  "targets": [ {
                      "channel": "string",
                      "schedulerUuid": "string",
                      "updatedAt": "2019-08-24T14:15:22Z",
                      "createdAt": "2019-08-24T14:15:22Z",
                      "schedulerSlackTargetUuid": "string" } ] } ] }
        """
        resp = self._request("get", f"schedulers/{project_uuid}/logs")
        if resp.ok:
            return resp.json()["results"]
        return {}

    def get_scheduler(self, scheduler_uuid: str):
        """
        Get details of a scheduler
        Return format:
            { "options": {
                "limit": 0,
                "formatted": true },
              "dashboardUuid": null,
              "savedChartUuid": "string",
              "cron": "string",
              "format": "csv",
              "createdBy": "string",
              "updatedAt": "2019-08-24T14:15:22Z",
              "createdAt": "2019-08-24T14:15:22Z",
              "message": "string",
              "name": "string",
              "schedulerUuid": "string",
              "targets": [ {
                  "channel": "string",
                  "schedulerUuid": "string",
                  "updatedAt": "2019-08-24T14:15:22Z",
                  "createdAt": "2019-08-24T14:15:22Z",
                  "schedulerSlackTargetUuid": "string" } ] }
        """
        resp = self._request("get", f"schedulers/{scheduler_uuid}")
        if resp.ok:
            return resp.json()["results"]
        return {}

    def get_scheduler_jobs(self, scheduler_uuid: str):
        """
        Get jobs scheduled by a scheduler
        Return format:
            [ { "id": "string",
                "date": "2019-08-24T14:15:22Z" } ]
        """
        resp = self._request("get", f"schedulers/{scheduler_uuid}/jobs")
        if resp.ok:
            return resp.json()["results"]
        return []

    def get_scheduler_job_status(self, job_id: str):
        """
        Get a generic job status
        Return format:
            { "status": "string" }
        """
        resp = self._request("get", f"schedulers/job/{job_id}/status")
        if resp.ok:
            return resp.json()["results"]
        return {}
