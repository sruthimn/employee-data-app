import datetime
import json
import os
import urllib.request

import boto3


def find_api_url(ecs, ec2, cluster, service, port=8000):
    arns = ecs.list_tasks(cluster=cluster, serviceName=service,
                          desiredStatus="RUNNING")["taskArns"]
    if not arns:
        raise RuntimeError(f"No running task in {service}")
    task = ecs.describe_tasks(cluster=cluster, tasks=arns[:1])["tasks"][0]
    eni = next(d["value"] for a in task["attachments"] for d in a["details"]
               if d["name"] == "networkInterfaceId")
    nic = ec2.describe_network_interfaces(NetworkInterfaceIds=[eni])["NetworkInterfaces"][0]
    return f"http://{nic['Association']['PublicIp']}:{port}"


def fetch_employees(base_url):
    with urllib.request.urlopen(f"{base_url}/employees", timeout=10) as resp:
        return json.loads(resp.read())


def build_key(load_date):
    return f"raw/employees/load_date={load_date}/employees.json"


def to_json_lines(rows):
    return "\n".join(json.dumps(row) for row in rows)


def lambda_handler(event, context):
    ecs = boto3.client("ecs")
    ec2 = boto3.client("ec2")
    s3 = boto3.client("s3")
    glue = boto3.client("glue")

    load_date = datetime.date.today().isoformat()
    api_url = find_api_url(ecs, ec2, os.environ["ECS_CLUSTER"], os.environ["ECS_SERVICE"])
    rows = fetch_employees(api_url)
    key = build_key(load_date)

    s3.put_object(
        Bucket=os.environ["BUCKET"],
        Key=key,
        Body=to_json_lines(rows).encode("utf-8"),
    )
    run = glue.start_job_run(
        JobName=os.environ["GLUE_JOB"],
        Arguments={"--LOAD_DATE": load_date},
    )
    return {"rows": len(rows), "api_url": api_url, "key": key, "glue_run_id": run["JobRunId"]}