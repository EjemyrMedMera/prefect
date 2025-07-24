import pytest
from prefect_gcp.credentials import GcpCredentials
from prefect_gcp.models.cloud_run_v2 import SecretKeySelector
from prefect_gcp.utilities import slugify_name
from prefect_gcp.workers.cloud_run_v2 import CloudRunWorkerJobV2Configuration

from prefect.utilities.dockerutils import get_prefect_image_name


@pytest.fixture
def job_body():
    return {
        "client": "prefect",
        "launchStage": None,
        "template": {
            "template": {
                "maxRetries": None,
                "timeout": None,
                "vpcAccess": {
                    "connector": None,
                },
                "containers": [
                    {
                        "env": [],
                        "command": None,
                        "args": "-m prefect.engine",
                        "resources": {
                            "limits": {
                                "cpu": None,
                                "memory": None,
                            },
                        },
                    },
                ],
            }
        },
    }


@pytest.fixture
def cloud_run_worker_v2_job_config(service_account_info, job_body):
    return CloudRunWorkerJobV2Configuration(
        name="my-job-name",
        job_body=job_body,
        credentials=GcpCredentials(service_account_info=service_account_info),
        region="us-central1",
        timeout=86400,
        env={"ENV1": "VALUE1", "ENV2": "VALUE2"},
    )


@pytest.fixture
def cloud_run_worker_v2_job_config_noncompliant_name(service_account_info, job_body):
    return CloudRunWorkerJobV2Configuration(
        name="MY_JOB_NAME",
        job_body=job_body,
        credentials=GcpCredentials(service_account_info=service_account_info),
        region="us-central1",
        timeout=86400,
        env={"ENV1": "VALUE1", "ENV2": "VALUE2"},
    )


class TestCloudRunWorkerJobV2Configuration:
    def test_project(self, cloud_run_worker_v2_job_config):
        assert cloud_run_worker_v2_job_config.project == "my_project"

    def test_job_name(self, cloud_run_worker_v2_job_config):
        assert cloud_run_worker_v2_job_config.job_name[:-33] == "my-job-name"

    def test_job_name_is_slug(self, cloud_run_worker_v2_job_config_noncompliant_name):
        assert cloud_run_worker_v2_job_config_noncompliant_name.job_name[
            :-33
        ] == slugify_name("MY_JOB_NAME")

    def test_job_name_different_after_retry(self, cloud_run_worker_v2_job_config):
        job_name_1 = cloud_run_worker_v2_job_config.job_name

        cloud_run_worker_v2_job_config._job_name = None

        job_name_2 = cloud_run_worker_v2_job_config.job_name

        assert job_name_1[:-33] == job_name_2[:-33]
        assert job_name_1 != job_name_2

    def test_populate_timeout(self, cloud_run_worker_v2_job_config):
        cloud_run_worker_v2_job_config._populate_timeout()

        assert (
            cloud_run_worker_v2_job_config.job_body["template"]["template"]["timeout"]
            == "86400s"
        )

    def test_populate_env(self, cloud_run_worker_v2_job_config):
        cloud_run_worker_v2_job_config._populate_env()

        env_vars = cloud_run_worker_v2_job_config.job_body["template"]["template"][
            "containers"
        ][0]["env"]

        # Check that both environment variables are present
        assert len(env_vars) == 2
        env_dict = {env["name"]: env["value"] for env in env_vars}
        assert env_dict == {"ENV1": "VALUE1", "ENV2": "VALUE2"}

    def test_populate_env_with_secrets(self, cloud_run_worker_v2_job_config):
        cloud_run_worker_v2_job_config.env_from_secrets = {
            "SECRET_ENV1": SecretKeySelector(secret="SECRET1", version="latest")
        }
        cloud_run_worker_v2_job_config._populate_env()

        env_vars = cloud_run_worker_v2_job_config.job_body["template"]["template"][
            "containers"
        ][0]["env"]

        # Check that all three environment variables are present
        assert len(env_vars) == 3

        # Check plain text variables
        plain_text_vars = {
            env["name"]: env["value"] for env in env_vars if "value" in env
        }
        assert plain_text_vars == {"ENV1": "VALUE1", "ENV2": "VALUE2"}

        # Check secret variable
        secret_vars = [env for env in env_vars if "valueSource" in env]
        assert len(secret_vars) == 1
        assert secret_vars[0]["name"] == "SECRET_ENV1"
        assert secret_vars[0]["valueSource"]["secretKeyRef"]["secret"] == "SECRET1"

    def test_populate_env_with_existing_envs(self, cloud_run_worker_v2_job_config):
        cloud_run_worker_v2_job_config.job_body["template"]["template"]["containers"][
            0
        ]["env"] = [{"name": "ENV0", "value": "VALUE0"}]
        cloud_run_worker_v2_job_config.env_from_secrets = {
            "SECRET_ENV1": SecretKeySelector(secret="SECRET1", version="latest")
        }
        cloud_run_worker_v2_job_config._populate_env()

        env_vars = cloud_run_worker_v2_job_config.job_body["template"]["template"][
            "containers"
        ][0]["env"]

        # Check that all four environment variables are present
        assert len(env_vars) == 4

        # Check plain text variables
        plain_text_vars = {
            env["name"]: env["value"] for env in env_vars if "value" in env
        }
        assert plain_text_vars == {"ENV0": "VALUE0", "ENV1": "VALUE1", "ENV2": "VALUE2"}

        # Check secret variable
        secret_vars = [env for env in env_vars if "valueSource" in env]
        assert len(secret_vars) == 1
        assert secret_vars[0]["name"] == "SECRET_ENV1"
        assert secret_vars[0]["valueSource"]["secretKeyRef"]["secret"] == "SECRET1"

    def test_populate_image_if_not_present(self, cloud_run_worker_v2_job_config):
        cloud_run_worker_v2_job_config._populate_image_if_not_present()

        assert (
            cloud_run_worker_v2_job_config.job_body["template"]["template"][
                "containers"
            ][0]["image"]
            == f"docker.io/{get_prefect_image_name()}"
        )

    def test_populate_or_format_command(self, cloud_run_worker_v2_job_config):
        cloud_run_worker_v2_job_config._populate_or_format_command()

        assert cloud_run_worker_v2_job_config.job_body["template"]["template"][
            "containers"
        ][0]["command"] == ["prefect", "flow-run", "execute"]

    def test_format_args_if_present(self, cloud_run_worker_v2_job_config):
        cloud_run_worker_v2_job_config._format_args_if_present()

        assert cloud_run_worker_v2_job_config.job_body["template"]["template"][
            "containers"
        ][0]["args"] == ["-m", "prefect.engine"]

    @pytest.mark.parametrize("vpc_access", [{"connector": None}, {}, None])
    def test_remove_vpc_access_if_connector_unset(
        self, cloud_run_worker_v2_job_config, vpc_access
    ):
        cloud_run_worker_v2_job_config.job_body["template"]["template"][
            "vpcAccess"
        ] = vpc_access

        cloud_run_worker_v2_job_config._remove_vpc_access_if_unset()

        assert (
            "vpcAccess"
            not in cloud_run_worker_v2_job_config.job_body["template"]["template"]
        )

    def test_remove_vpc_access_originally_not_present(
        self, cloud_run_worker_v2_job_config
    ):
        cloud_run_worker_v2_job_config.job_body["template"]["template"].pop("vpcAccess")

        cloud_run_worker_v2_job_config._remove_vpc_access_if_unset()

        assert (
            "vpcAccess"
            not in cloud_run_worker_v2_job_config.job_body["template"]["template"]
        )

    def test_vpc_access_left_alone_if_connector_set(
        self, cloud_run_worker_v2_job_config
    ):
        cloud_run_worker_v2_job_config.job_body["template"]["template"]["vpcAccess"][
            "connector"
        ] = "projects/my_project/locations/us-central1/connectors/my-connector"

        cloud_run_worker_v2_job_config._remove_vpc_access_if_unset()

        assert cloud_run_worker_v2_job_config.job_body["template"]["template"][
            "vpcAccess"
        ] == {
            "connector": "projects/my_project/locations/us-central1/connectors/my-connector"  # noqa E501
        }

    def test_vpc_access_left_alone_if_network_config_set(
        self, cloud_run_worker_v2_job_config
    ):
        cloud_run_worker_v2_job_config.job_body["template"]["template"]["vpcAccess"][
            "networkInterfaces"
        ] = [{"network": "projects/my_project/global/networks/my-network"}]

        cloud_run_worker_v2_job_config._remove_vpc_access_if_unset()

        assert cloud_run_worker_v2_job_config.job_body["template"]["template"][
            "vpcAccess"
        ] == {
            "connector": None,
            "networkInterfaces": [
                {"network": "projects/my_project/global/networks/my-network"}
            ],
        }

    def test_configure_cloudsql_volumes_no_instances(
        self, cloud_run_worker_v2_job_config
    ):
        cloud_run_worker_v2_job_config.cloudsql_instances = []
        cloud_run_worker_v2_job_config._configure_cloudsql_volumes()

        template = cloud_run_worker_v2_job_config.job_body["template"]["template"]

        assert "volumes" not in template
        assert "volumeMounts" not in template["containers"][0]

    def test_configure_cloudsql_volumes_preserves_existing_volumes(
        self, cloud_run_worker_v2_job_config
    ):
        template = cloud_run_worker_v2_job_config.job_body["template"]["template"]
        template["volumes"] = [{"name": "existing-volume", "emptyDir": {}}]
        template["containers"][0]["volumeMounts"] = [
            {"name": "existing-volume", "mountPath": "/existing"}
        ]

        cloud_run_worker_v2_job_config.cloudsql_instances = ["project:region:instance1"]
        cloud_run_worker_v2_job_config._configure_cloudsql_volumes()

        assert len(template["volumes"]) == 2
        assert template["volumes"][0] == {"name": "existing-volume", "emptyDir": {}}
        assert template["volumes"][1] == {
            "name": "cloudsql",
            "cloudSqlInstance": {"instances": ["project:region:instance1"]},
        }

        assert len(template["containers"][0]["volumeMounts"]) == 2
        assert template["containers"][0]["volumeMounts"][0] == {
            "name": "existing-volume",
            "mountPath": "/existing",
        }
        assert template["containers"][0]["volumeMounts"][1] == {
            "name": "cloudsql",
            "mountPath": "/cloudsql",
        }

    def test_prepare_for_flow_run_configures_cloudsql(
        self, cloud_run_worker_v2_job_config
    ):
        cloud_run_worker_v2_job_config.cloudsql_instances = ["project:region:instance1"]

        class MockFlowRun:
            id = "test-id"
            name = "test-run"

        cloud_run_worker_v2_job_config.prepare_for_flow_run(
            flow_run=MockFlowRun(), deployment=None, flow=None
        )

        template = cloud_run_worker_v2_job_config.job_body["template"]["template"]

        assert any(
            vol["name"] == "cloudsql"
            and vol["cloudSqlInstance"]["instances"] == ["project:region:instance1"]
            for vol in template["volumes"]
        )
        assert any(
            mount["name"] == "cloudsql" and mount["mountPath"] == "/cloudsql"
            for mount in template["containers"][0]["volumeMounts"]
        )

    def test_populate_env_with_prefect_api_key_secret(
        self, cloud_run_worker_v2_job_config
    ):
        cloud_run_worker_v2_job_config.prefect_api_key_secret = SecretKeySelector(
            secret="prefect-api-key", version="latest"
        )
        cloud_run_worker_v2_job_config._populate_env()

        env_vars = cloud_run_worker_v2_job_config.job_body["template"]["template"][
            "containers"
        ][0]["env"]

        assert {"name": "ENV1", "value": "VALUE1"} in env_vars
        assert {"name": "ENV2", "value": "VALUE2"} in env_vars
        assert {
            "name": "PREFECT_API_KEY",
            "valueSource": {
                "secretKeyRef": {"secret": "prefect-api-key", "version": "latest"}
            },
        } in env_vars

    def test_populate_env_with_prefect_api_auth_string_secret(
        self, cloud_run_worker_v2_job_config
    ):
        cloud_run_worker_v2_job_config.prefect_api_auth_string_secret = (
            SecretKeySelector(secret="prefect-auth-string", version="latest")
        )
        cloud_run_worker_v2_job_config._populate_env()

        env_vars = cloud_run_worker_v2_job_config.job_body["template"]["template"][
            "containers"
        ][0]["env"]

        assert {"name": "ENV1", "value": "VALUE1"} in env_vars
        assert {"name": "ENV2", "value": "VALUE2"} in env_vars
        assert {
            "name": "PREFECT_API_AUTH_STRING",
            "valueSource": {
                "secretKeyRef": {"secret": "prefect-auth-string", "version": "latest"}
            },
        } in env_vars

    def test_populate_env_with_both_prefect_secrets(
        self, cloud_run_worker_v2_job_config
    ):
        cloud_run_worker_v2_job_config.prefect_api_key_secret = SecretKeySelector(
            secret="prefect-api-key", version="latest"
        )
        cloud_run_worker_v2_job_config.prefect_api_auth_string_secret = (
            SecretKeySelector(secret="prefect-auth-string", version="latest")
        )
        cloud_run_worker_v2_job_config._populate_env()

        env_vars = cloud_run_worker_v2_job_config.job_body["template"]["template"][
            "containers"
        ][0]["env"]

        assert {"name": "ENV1", "value": "VALUE1"} in env_vars
        assert {"name": "ENV2", "value": "VALUE2"} in env_vars
        assert {
            "name": "PREFECT_API_KEY",
            "valueSource": {
                "secretKeyRef": {"secret": "prefect-api-key", "version": "latest"}
            },
        } in env_vars
        assert {
            "name": "PREFECT_API_AUTH_STRING",
            "valueSource": {
                "secretKeyRef": {"secret": "prefect-auth-string", "version": "latest"}
            },
        } in env_vars

    def test_populate_env_with_all_secret_types(self, cloud_run_worker_v2_job_config):
        cloud_run_worker_v2_job_config.env_from_secrets = {
            "SECRET_ENV1": SecretKeySelector(secret="SECRET1", version="latest")
        }
        cloud_run_worker_v2_job_config.prefect_api_key_secret = SecretKeySelector(
            secret="prefect-api-key", version="latest"
        )
        cloud_run_worker_v2_job_config.prefect_api_auth_string_secret = (
            SecretKeySelector(secret="prefect-auth-string", version="latest")
        )
        cloud_run_worker_v2_job_config._populate_env()

        env_vars = cloud_run_worker_v2_job_config.job_body["template"]["template"][
            "containers"
        ][0]["env"]

        assert {"name": "ENV1", "value": "VALUE1"} in env_vars
        assert {"name": "ENV2", "value": "VALUE2"} in env_vars
        assert {
            "name": "SECRET_ENV1",
            "valueSource": {"secretKeyRef": {"secret": "SECRET1", "version": "latest"}},
        } in env_vars
        assert {
            "name": "PREFECT_API_KEY",
            "valueSource": {
                "secretKeyRef": {"secret": "prefect-api-key", "version": "latest"}
            },
        } in env_vars
        assert {
            "name": "PREFECT_API_AUTH_STRING",
            "valueSource": {
                "secretKeyRef": {"secret": "prefect-auth-string", "version": "latest"}
            },
        } in env_vars

    def test_populate_env_with_deduplication(self, cloud_run_worker_v2_job_config):
        """Test comprehensive environment variable population and deduplication.

        Tests all precedence levels and deduplication logic:
        1. Secrets (highest precedence)
        2. Plain text env vars (medium precedence)
        3. Existing env vars in job body (lowest precedence)
        """
        # Set up existing env vars in job body (lowest precedence)
        cloud_run_worker_v2_job_config.job_body["template"]["template"]["containers"][
            0
        ]["env"] = [
            {"name": "EXISTING_ONLY", "value": "existing_value"},
            {"name": "OVERRIDE_BY_PLAIN", "value": "existing_override_me"},
            {"name": "OVERRIDE_BY_SECRET", "value": "existing_override_me"},
            {"name": "DUPLICATE_EXISTING", "value": "first_existing"},
            {"name": "DUPLICATE_EXISTING", "value": "second_existing"},
        ]

        # Set up plain text env vars (medium precedence)
        cloud_run_worker_v2_job_config.env = {
            "PLAIN_ONLY": "plain_value",
            "OVERRIDE_BY_PLAIN": "plain_overrides_existing",
            "OVERRIDE_BY_SECRET": "plain_gets_overridden",
        }

        # Set up secret env vars (highest precedence)
        cloud_run_worker_v2_job_config.env_from_secrets = {
            "SECRET_ONLY": SecretKeySelector(secret="SECRET1", version="latest"),
            "OVERRIDE_BY_SECRET": SecretKeySelector(secret="SECRET2", version="1"),
        }
        cloud_run_worker_v2_job_config._populate_env()
        cloud_run_worker_v2_job_config._deduplicate_env()

        env_vars = cloud_run_worker_v2_job_config.job_body["template"]["template"][
            "containers"
        ][0]["env"]

        # Should have 6 unique variables (duplicates removed by deduplication)
        env_names = [env["name"] for env in env_vars]
        assert len(set(env_names)) == 6
        assert len(env_vars) == 6

        # Test precedence: secrets win
        override_by_secret = next(
            env for env in env_vars if env["name"] == "OVERRIDE_BY_SECRET"
        )
        assert "valueSource" in override_by_secret
        assert override_by_secret["valueSource"]["secretKeyRef"]["secret"] == "SECRET2"

        # Test precedence: plain text wins over existing
        override_by_plain = next(
            env for env in env_vars if env["name"] == "OVERRIDE_BY_PLAIN"
        )
        assert override_by_plain["value"] == "plain_overrides_existing"

        # Test existing values remain when not overridden
        existing_only = next(env for env in env_vars if env["name"] == "EXISTING_ONLY")
        assert existing_only["value"] == "existing_value"

        # Test plain text only variables
        plain_only = next(env for env in env_vars if env["name"] == "PLAIN_ONLY")
        assert plain_only["value"] == "plain_value"

        # Test secret only variables
        secret_only = next(env for env in env_vars if env["name"] == "SECRET_ONLY")
        assert "valueSource" in secret_only
        assert secret_only["valueSource"]["secretKeyRef"]["secret"] == "SECRET1"

        # Test deduplication keeps last occurrence
        duplicate_existing = next(
            env for env in env_vars if env["name"] == "DUPLICATE_EXISTING"
        )
        assert duplicate_existing["value"] == "second_existing"

    def test_populate_env_without_deduplication(self, cloud_run_worker_v2_job_config):
        """Test that _populate_env() creates duplicates which are then removed by _deduplicate_env()."""
        # Set up scenario with duplicates
        cloud_run_worker_v2_job_config.job_body["template"]["template"]["containers"][
            0
        ]["env"] = [
            {"name": "SHARED_VAR", "value": "existing_value"},
        ]

        cloud_run_worker_v2_job_config.env = {
            "SHARED_VAR": "plain_text_value",
        }

        cloud_run_worker_v2_job_config.env_from_secrets = {
            "SHARED_VAR": SecretKeySelector(secret="SECRET1", version="latest"),
        }

        # Call only _populate_env() - should create duplicates
        cloud_run_worker_v2_job_config._populate_env()

        env_vars = cloud_run_worker_v2_job_config.job_body["template"]["template"][
            "containers"
        ][0]["env"]

        # Should have 3 total variables (with duplicates)
        assert len(env_vars) == 3
        # But only 1 unique name
        env_names = [env["name"] for env in env_vars]
        assert len(set(env_names)) == 1
        assert all(name == "SHARED_VAR" for name in env_names)

        # Now call _deduplicate_env() to remove duplicates
        cloud_run_worker_v2_job_config._deduplicate_env()

        env_vars_after = cloud_run_worker_v2_job_config.job_body["template"][
            "template"
        ]["containers"][0]["env"]

        # Should now have only 1 variable (duplicates removed)
        assert len(env_vars_after) == 1
        assert env_vars_after[0]["name"] == "SHARED_VAR"
        # Should be the secret version (highest precedence)
        assert "valueSource" in env_vars_after[0]
        assert env_vars_after[0]["valueSource"]["secretKeyRef"]["secret"] == "SECRET1"
