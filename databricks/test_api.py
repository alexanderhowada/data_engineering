# Databricks notebook source
import re
import json
import copy
import unittest
import requests

from wmc_lib.databricks.api import DatabricksJobsAPI, DatabricksGitAPI

# COMMAND ----------

JOB_CONFIG = {
    "settings": {
        "name": "test_wmc_lib_databricks_api_jobs",
        "email_notifications": {
            "on_failure": [
                # "3fd1ec0f.interpublic.onmicrosoft.com@amer.teams.ms"
            ],
            "no_alert_for_skipped_runs": True
        },
        "webhook_notifications": {},
        "notification_settings": {
            "no_alert_for_skipped_runs": True,
            "no_alert_for_canceled_runs": True
        },
        "timeout_seconds": 0,
        "schedule": {
            "quartz_cron_expression": "42 0 7 * * ?",
            "timezone_id": "America/Sao_Paulo",
            "pause_status": "PAUSED"
        },
        "max_concurrent_runs": 1,
        "tasks": [
            {
                "task_key": "test1",
                "notebook_task": {
                    "notebook_path": "/wmc/unittest/test",
                    "source": "WORKSPACE"
                },
                "job_cluster_key": "test_wmc_lib_databricks_api_jobs",
                "max_retries": 1,
                "min_retry_interval_millis": 900000,
                "retry_on_timeout": True,
                "timeout_seconds": 2700,
                "email_notifications": {},
                "notification_settings": {
                    "no_alert_for_skipped_runs": False,
                    "no_alert_for_canceled_runs": False,
                    "alert_on_last_attempt": False
                }
            },
            {
                "task_key": "test2",
                "notebook_task": {
                    "notebook_path": "/wmc/unittest/test",
                    "source": "WORKSPACE"
                },
                "job_cluster_key": "test_wmc_lib_databricks_api_jobs",
                "max_retries": 1,
                "min_retry_interval_millis": 900000,
                "retry_on_timeout": False,
                "timeout_seconds": 2700,
                "email_notifications": {},
                "notification_settings": {
                    "no_alert_for_skipped_runs": False,
                    "no_alert_for_canceled_runs": False,
                    "alert_on_last_attempt": False
                }
            }
        ],
        "job_clusters": [
            {
                "job_cluster_key": "test_wmc_lib_databricks_api_jobs",
                "new_cluster": {
                    "cluster_name": "",
                    "spark_version": "12.2.x-scala2.12",
                    "spark_conf": {
                        "spark.databricks.delta.schema.autoMerge.enabled": "true",
                        "spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite": "true",
                        "spark.databricks.preemption.enabled": "true",
                        "spark.master": "local[*, 4]",
                        "spark.databricks.preemption.threshold": "0.5",
                        "spark.databricks.cluster.profile": "singleNode",
                        "spark.sql.shuffle.partitions": "10"
                    },
                    "gcp_attributes": {
                        "use_preemptible_executors": False,
                        "google_service_account": "databricks@long-operator-271320.iam.gserviceaccount.com",
                        "availability": "ON_DEMAND_GCP",
                        "zone_id": "HA"
                    },
                    "node_type_id": "n2-highmem-2",
                    "driver_node_type_id": "n2-highmem-2",
                    "custom_tags": {
                        "team": "product",
                        "ResourceClass": "SingleNode"
                    },
                    "enable_elastic_disk": True,
                    "init_scripts": [
                        {
                            "gcs": {
                                "destination": "gs://databricks-long-operator/init_scripts/all.sh"
                            }
                        }
                    ],
                    "policy_id": "846314FA9A000017",
                    "data_security_mode": "SINGLE_USER",
                    "runtime_engine": "STANDARD",
                    "num_workers": 0
                }
            }
        ],
        "git_source": {
            "git_url": "https://github.com/wmc-team/loreal.git",
            "git_provider": "gitHub",
            "git_branch": "main"
        },
        "format": "MULTI_TASK"
    }
}

# COMMAND ----------

# jobs_api = DatabricksJobsAPI(
#     dbutils.secrets.get(scope="pat", key="host"),
#     dbutils.secrets.get(scope="pat", key="wmc_eng_team_default")
# )
# job_id = jobs_api("create", json=JOB_CONFIG["settings"]).json()["job_id"]
# print(job_id)

# COMMAND ----------

class TestDatabricksJobsAPI(unittest.TestCase):

    def setUp(self):
        self.jobs_api = DatabricksJobsAPI(
            dbutils.secrets.get(scope="pat", key="host"),
            dbutils.secrets.get(scope="pat", key="wmc_eng_team_default")
        )
        self.job_id = self.jobs_api("create", json=JOB_CONFIG["settings"]).json()["job_id"]

    def tearDown(self):
        self.jobs_api("delete", json={"job_id": self.job_id})

    def test_list_empty(self):
        """Test is get_jobs_list returns an empty list."""

        res = self.jobs_api.get_jobs_list(
            j={"limit": 25, "name": "pqiwoerpqoiwuerpqiowuerpqoiwuerpqoiuwe"}
        )

        self.assertTrue(isinstance(res, list))
        self.assertEqual(len(res), 0)
        
    def test_list(self):
        """Test is get_jobs_list returns."""

        res = self.jobs_api.get_jobs_list()
        names = [r["settings"]["name"] for r in res]

        self.assertTrue(JOB_CONFIG["settings"]["name"] in names)
        self.assertTrue(len(res) > 0)

    def test_create_existing(self):
        """Test if raises an error when creating a job with an existing name."""

        with self.assertRaises(Exception, msg="Job already exists."):
            self.jobs_api("create", json=JOB_CONFIG["settings"])

    def test_get_job(self):
        """Test if has the same settings."""

        config = self.jobs_api.get_job(self.job_id)
        config1 = re.sub("\s", "", json.dumps(config["settings"]))

        config2 = copy.deepcopy(JOB_CONFIG)
        del config2["settings"]["email_notifications"]["on_failure"]
        config2 = re.sub("\s", "", json.dumps(config2["settings"]))

        self.assertEqual(config1, config2)

    def test_get_job_id(self):
        """Test get job id for the same id."""

        job_id = self.jobs_api.get_job_id(JOB_CONFIG["settings"]["name"])

        self.assertEqual(job_id, self.job_id)

    def test_get_job_id(self):
        """Test get job id for the same id."""

        job_id = self.jobs_api.get_job_id(JOB_CONFIG["settings"]["name"])

        self.assertEqual(job_id, self.job_id)

    def test_get_job_id_multiple_names(self):
        """Assert it raises Exception when there is multiple jobs with the same name."""

        url = self.jobs_api.host + self.jobs_api.endpoints["create"]["endpoint"]
        rr = requests.post(
            url, json=JOB_CONFIG["settings"],
            headers=self.jobs_api.get_headers()
        )

        with self.assertRaises(Exception):
            self.jobs_api.get_job_id(JOB_CONFIG["settings"]["name"])

        self.jobs_api("delete", json={"job_id": rr.json()["job_id"]})

    def test_create(self):
        """Test if is creating a new job.
        
        This test creates a new workflow and compare the number of workflows
        before and after the creation.
        It also checks if the new workflow name is among the workflows.
        """

        before_jobs_list = self.jobs_api.get_jobs_list()

        # Create a new workflow
        job_config = copy.deepcopy(JOB_CONFIG)
        job_config["settings"]["name"] += "_asdf"
        job_id = self.jobs_api("create", json=job_config["settings"]).json()["job_id"]

        after_jobs_list = self.jobs_api.get_jobs_list()
        names_list = [j["settings"]["name"] for j in after_jobs_list]

        self.jobs_api("delete", json={"job_id": job_id})

        self.assertTrue(job_config["settings"]["name"] in names_list)
        self.assertEqual(len(before_jobs_list)+1, len(after_jobs_list))

    def test_update_add_task(self):
        """Update a workflow by creating two new tasks."""

        jobs_config = copy.deepcopy(JOB_CONFIG)
        tasks = copy.deepcopy(JOB_CONFIG["settings"]["tasks"])

        for i in range(len(tasks)):
            tasks[i]["task_key"] += "_asdf"

        jobs_config["settings"]["tasks"] += tasks

        self.jobs_api(
            "update",
            json={"new_settings": jobs_config["settings"],
                  "job_id": self.job_id}
        )

        jobs_config = self.jobs_api.get_job(self.job_id)

        self.assertEqual(len(jobs_config["settings"]["tasks"]), 4)

    def test_update_remove_task(self):
        """Update a workflow by removing a task. Nothing should happen."""

        jobs_config = copy.deepcopy(JOB_CONFIG)
        jobs_config["settings"]["tasks"][0]["timeout_seconds"] = 1000
        del jobs_config["settings"]["tasks"][-1]

        self.jobs_api(
            "update",
            json={
                "new_settings": jobs_config["settings"],
                "job_id": self.job_id
            }
        )

        jobs_config = self.jobs_api.get_job(self.job_id)

        self.assertEqual(len(jobs_config["settings"]["tasks"]), 2)
        self.assertEqual(jobs_config["settings"]["tasks"][0]["timeout_seconds"], 1000)

    def test_reset(self):
        """Test if resets the workflow by removing one task."""

        jobs_config = copy.deepcopy(JOB_CONFIG)
        jobs_config["settings"]["tasks"][0]["timeout_seconds"] = 999
        del jobs_config["settings"]["tasks"][-1]

        self.jobs_api(
            "reset",
            json={
                "new_settings": jobs_config["settings"],
                "job_id": self.job_id
            }
        )

        jobs_config = self.jobs_api.get_job(self.job_id)

        self.assertEqual(len(jobs_config["settings"]["tasks"]), 1)
        self.assertEqual(jobs_config["settings"]["tasks"][0]["timeout_seconds"], 999)

class TestDatabricksGitAPI(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.api = DatabricksGitAPI(
            dbutils.secrets.get(scope="pat", key="host"),
            dbutils.secrets.get(scope="pat", key="wmc_eng_team_default")
        )

    def test_get(self):
        j1 = self.api.get()
        j2 = self.api("get").json()
        self.assertEqual(json.dumps(j1), json.dumps(j2))


if __name__ == '__main__':
    r = unittest.main(argv=[''], verbosity=2, exit=False)
    assert r.result.wasSuccessful(), 'Test failed; see logs above'    

# COMMAND ----------


