import json
import os
import subprocess
import time
from datetime import datetime, timezone

import boto3
import pusher
from bson import ObjectId
from pymongo import MongoClient

client = MongoClient(os.environ.get("DB_CONN_STRING"))
db = client["core"]

sqs = boto3.client('sqs', region_name="us-west-1")
SQS_URL = os.environ.get("SQS_URL")

ec2 = boto3.client('ec2', region_name="us-west-1")

pusher_client = pusher.Pusher(
  app_id=os.environ.get("PUSHER_APP_ID"),
  key=os.environ.get("PUSHER_KEY"),
  secret=os.environ.get("PUSHER_SECRET"),
  cluster='us3',
  ssl=True
)

instance_id = subprocess.run(['ec2metadata', '--instance-id'], capture_output=True).stdout.decode("utf-8").replace("\n", "")
instance_type = subprocess.run(['ec2metadata', '--instance-type'], capture_output=True).stdout.decode("utf-8").replace("\n", "")
instance_region = subprocess.run(['ec2metadata', '--availability-zone'], capture_output=True).stdout.decode("utf-8").replace("\n", "")


# Update job status in MongoDB and send Pusher message
def update_status(container_id, job_id, status):
    col = db["jobs"]

    result = col.update_one({ "_id": job_id, "containerId": container_id }, { "$set": {
        "status": status
    }})

    timestamp = datetime.now(timezone.utc)
    if status == "STARTED":
        col.update_one({ "_id": job_id, "containerId": container_id }, { "$set": {
            "started": timestamp
         }})

    if status == "COMPLETED" or status == "FAILED" or status == "BUILD_FAILED":
        col.update_one({ "_id": job_id, "containerId": container_id }, { "$set": {
            "ended": timestamp
         }})

    if result.modified_count > 0:
        pusher_client.trigger(str(container_id), 'job-status', {
            "jobId": str(job_id),
            "status": status,
            "timestamp": str(timestamp)
        })

    return result.modified_count > 0


# Register node details in MongoDB
def register_node():
    nodes = db["nodes"]
    jobs = db["jobs"]

    timestamp = datetime.now(timezone.utc)
    
    node = nodes.find_one_and_update({ "pending": True, "alive": False }, { "$set": {
        "pending": False,
        "alive": True,
        "started": timestamp,
        "instanceId": instance_id,
        "instanceType": instance_type,
        "instanceRegion": instance_region
    }}, return_document=True)

    if node:
        node_jobs = list(jobs.find({ "node": node["_id"], "status": { "$nin": ["COMPLETED", "FAILED", "BUILD_FAILED"] }}))
        if len(node_jobs) > 0:
            for job in node_jobs:
                update_status(job["containerId"], job["_id"], "NODE_STARTED")
            for job in node_jobs:
                update_status(job["containerId"], job["_id"], "PENDING")
        return node["_id"]
    return None


# Update node status in MongoDB and de-provision EC2 instance
def shutdown_node(node_id):
    col = db["nodes"]

    timestamp = datetime.now(timezone.utc)

    col.update_one({ "_id": node_id }, { "$set": {
        "alive": False,
        "killed": timestamp, 
    }})
    
    ec2.terminate_instances(InstanceIds=[instance_id])


# Dynamically generate Dockerfile for passed job
def generate_dockerfile(container_id):

    def getPath(folder, container):
        path = []
        if folder != "~":
            cur_folder = container["folders"][folder]
            if not cur_folder:
                return -1
            else:
                while True:
                    path.insert(0, cur_folder["name"])
                    if cur_folder["parent"] == "~":
                        break
                    cur_folder = container["folders"][cur_folder["parent"]]
        return f"{'/'.join(path)}{'/' if len(path) > 0 else ''}"

    containers = db["containers"]
    files = db["files"]
  
    container = containers.find_one({ "_id": container_id })
    entry_point = files.find_one({ "_id": container["entryPoint"] })

    parts = entry_point["name"].split(".")
    
    template_path = f"/opt/cloudcontain-job-execution/{parts[1].lower()}.dockerfile"
    with open(template_path, "r") as template_file:
        template = template_file.read()

    content = template.replace("{{ENTRY_POINT_FILENAME}}", parts[0])
    content = content.replace("{{ENTRY_POINT_FILE}}", entry_point["name"])
    content = content.replace("{{ENTRY_POINT_PATH}}", getPath(str(entry_point["folder"]), container) + entry_point["name"])

    output_path = os.path.join(f"/tmp/cloudcontain-jobs/{str(container_id)}", "Dockerfile")
    with open(output_path, "w") as output:
        output.write(content)

    return parts[1].lower()


# Emit log via Pusher and store in MongoDB
def emit_log(container_id, job_id, content, level="stdout"):
    logs = db["logs"]

    timestamp = datetime.now(timezone.utc)
    ns =  time.perf_counter_ns()

    pusher_client.trigger(str(container_id), 'job-output', {
        "jobId": str(job_id),
        "content": content,
        "timestamp": str(timestamp),
        "level": level,
    })

    logs.insert_one({
        "containerId": container_id,
        "jobId": job_id,
        "content": content,
        "timestamp": timestamp,
        "ns": ns,
        "level": level,
    })


# Get level and line content from log line
def get_line_info(line):
    level = "stderr" if line.startswith("[STDOUT] [STDERR]") else "stdout"
    line = line[18:] if line.startswith("[STDOUT] [STDERR]") else line[9:] if line.startswith("[STDOUT]") else line

    return level, line


# Get incoming job requests
def get_incoming_request():
    response = sqs.receive_message(
        QueueUrl=SQS_URL,
        MaxNumberOfMessages=1,
        WaitTimeSeconds=20,
        VisibilityTimeout=1800
    )
    job_request = response.get('Messages', [])

    if not job_request:
        return None

    receipt_handle = job_request[0]["ReceiptHandle"]
    
    job = json.loads(job_request[0]["Body"])
    job_id = ObjectId(job["jobId"])
    container_id = ObjectId(job["containerId"])

    return receipt_handle, job_id, container_id


# Check if job has been previously processed
def is_job_processed(job_id):
    jobs = db["jobs"]
    return jobs.count_documents({ "_id": job_id, "status": { "$nin": ["PENDING"] }}) > 0


# Delete job from queue
def delete_job_from_queue(receipt_handle):
    sqs.delete_message(
        QueueUrl=SQS_URL,
        ReceiptHandle=receipt_handle
    )


# Clean temporary job files and Docker images
def clean_tmp_env(container_id, job_id):
    subprocess.run(["rm", "-rf", f"/tmp/cloudcontain-jobs/{str(container_id)}"])
    subprocess.run(["docker", "kill", f"job-{str(job_id)}"])
    subprocess.run(["docker", "rm", "-f", f"job-{str(job_id)}"])
    subprocess.run(["docker", "rmi", "-f", f"job-{str(job_id)}"])


# Register instance and listen for jobs
if __name__ == "__main__":

    node_id = register_node()
    last_activity = int(time.time()) 
 
    while True:

        now = int(time.time())
        if now - last_activity >= 1800:
            shutdown_node(node_id)
            break

        try:
            
            if not (request := get_incoming_request()):
                continue

            receipt_handle, job_id, container_id = request
            last_activity = int(time.time())

            # Check job not yet processed
            if is_job_processed(job_id):
                delete_job_from_queue(receipt_handle)
                continue
        
            # Notify Pusher build is starting
            update_status(container_id, job_id, "STARTED")

            # Begin cloning container files
            update_status(container_id, job_id, "CLONING")
            subprocess.run(["aws", "s3", "sync", f"s3://cloudcontain-containers/{str(container_id)}/project", f"/tmp/cloudcontain-jobs/{str(container_id)}"]) 

            # Build Docker container
            update_status(container_id, job_id, "CONTAINERIZING")
            generate_dockerfile(container_id)

            build_process = subprocess.Popen(
                ["docker", "build", "-t", f"job-{str(job_id)}", f"/tmp/cloudcontain-jobs/{str(container_id)}"],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                universal_newlines=True,
                bufsize=1
            ) 
    
            try:
                for line in build_process.stdout:
                    emit_log(container_id, job_id, line, level="build")
            except Exception as e:
                print(e)
            finally:
                build_process.stdout.close()
                build_code = build_process.wait()

                # Check if build failed
                if build_code != 0:
                    update_status(container_id, job_id, "BUILD_FAILED")
                    subprocess.run(["rm", "-rf", f"/tmp/cloudcontain-jobs/{str(container_id)}"])
                    delete_job_from_queue(receipt_handle)
                    continue
                
                # Begin docker run of container files
                update_status(container_id, job_id, "RUNNING")
                job_process = subprocess.Popen(
                    ["docker", "run", "--rm", "-it", "--memory=512m", "--cpus=1", "--name", f"job-{str(job_id)}", f"job-{str(job_id)}"],
                    stdout=subprocess.PIPE,
                    universal_newlines=True,
                    bufsize=1
                )

                try:
                    for line in job_process.stdout:
                        level, line = get_line_info(line)
                        emit_log(container_id, job_id, line, level=level)
                except Exception as e:
                    print(e)
                finally:
                    job_process.stdout.close()
                    exit_code = job_process.wait()

                    # Clean up tmp files and remove docker image
                    update_status(container_id, job_id, "CLEANING")
                    clean_tmp_env(container_id, job_id)

                    # Notify Pusher of success/failure
                    update_status(container_id, job_id, "COMPLETED" if exit_code == 0 else "FAILED")

                # Remove queue item once execution is completed
                delete_job_from_queue(receipt_handle)

        except Exception as e:
            print(e)
