import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "lambda"))
import index  # noqa: E402


class FakeECS:
    def __init__(self, task_arns):
        self.task_arns = task_arns

    def list_tasks(self, cluster, serviceName, desiredStatus):
        return {"taskArns": self.task_arns}

    def describe_tasks(self, cluster, tasks):
        return {"tasks": [{"attachments": [{"details": [
            {"name": "subnetId", "value": "subnet-1"},
            {"name": "networkInterfaceId", "value": "eni-123"},
        ]}]}]}


class FakeEC2:
    def describe_network_interfaces(self, NetworkInterfaceIds):
        assert NetworkInterfaceIds == ["eni-123"]
        return {"NetworkInterfaces": [{"Association": {"PublicIp": "1.2.3.4"}}]}


class FakeS3:
    def __init__(self):
        self.saved = {}

    def put_object(self, Bucket, Key, Body):
        self.saved[(Bucket, Key)] = Body


class FakeGlue:
    def __init__(self):
        self.started = []

    def start_job_run(self, JobName, Arguments):
        self.started.append((JobName, Arguments))
        return {"JobRunId": "jr_123"}


def test_build_key():
    assert index.build_key("2026-11-02") == "raw/employees/load_date=2026-11-02/employees.json"


def test_to_json_lines():
    rows = [{"id": 1, "salary": 50000}, {"id": 2, "salary": 60000}]
    lines = index.to_json_lines(rows).split("\n")
    assert len(lines) == 2
    assert json.loads(lines[1])["id"] == 2


def test_find_api_url():
    url = index.find_api_url(FakeECS(["task-1"]), FakeEC2(), "cluster", "service")
    assert url == "http://1.2.3.4:8000"


def test_find_api_url_no_task():
    with pytest.raises(RuntimeError):
        index.find_api_url(FakeECS([]), FakeEC2(), "cluster", "service")


def test_handler_saves_to_s3_and_starts_glue(monkeypatch):
    fakes = {"ecs": FakeECS(["task-1"]), "ec2": FakeEC2(), "s3": FakeS3(), "glue": FakeGlue()}
    monkeypatch.setattr(index.boto3, "client", lambda name: fakes[name])
    monkeypatch.setattr(index, "fetch_employees", lambda url: [{"id": 1, "salary": 50000}])
    monkeypatch.setenv("ECS_CLUSTER", "employee-api-cluster")
    monkeypatch.setenv("ECS_SERVICE", "employee-api-service")
    monkeypatch.setenv("BUCKET", "data-bucket")
    monkeypatch.setenv("GLUE_JOB", "employee-data-transform")

    result = index.lambda_handler({}, None)

    assert result["rows"] == 1
    assert result["api_url"] == "http://1.2.3.4:8000"
    assert ("data-bucket", result["key"]) in fakes["s3"].saved
    assert fakes["glue"].started[0][0] == "employee-data-transform"