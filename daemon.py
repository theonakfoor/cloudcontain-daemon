import subprocess
from pymongo import MongoClient
import boto3
import pusher
import time
from datetime import datetime, timezone
import os
import json
from bson import ObjectId


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

instanceId = subprocess.run(['ec2metadata', '--instance-id'], capture_output=True).stdout.decode("utf-8").replace("\n", "")
instanceType = subprocess.run(['ec2metadata', '--instance-type'], capture_output=True).stdout.decode("utf-8").replace("\n", "")
instanceRegion = subprocess.run(['ec2metadata', '--availability-zone'], capture_output=True).stdout.decode("utf-8").replace("\n", "")


# Update job status in MongoDB and send Pusher message
def update_status(containerId, jobId, status):
    col = db["jobs"]
    
    containerId = containerId if isinstance(containerId, ObjectId) else ObjectId(containerId)
    jobId = jobId if isinstance(jobId, ObjectId) else ObjectId(jobId)

    result = col.update_one({ "_id": jobId, "containerId": containerId }, { "$set": {
        "status": status
    }})

    timestamp = datetime.now(timezone.utc)
    if status == "STARTED":
        col.update_one({ "_id": jobId, "containerId": containerId }, { "$set": {
            "started": timestamp
         }})

    if status == "COMPLETED" or status == "FAILED" or status == "BUILD_FAILED":
        col.update_one({ "_id": jobId, "containerId": containerId }, { "$set": {
            "ended": timestamp
         }})

    if result.modified_count > 0:
        pusher_client.trigger(str(containerId), 'job-status', {
            "jobId": str(jobId),
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
        "instanceId": instanceId,
        "instanceType": instanceType,
        "instanceRegion": instanceRegion
    }}, return_document=True)

    if node:
        node_jobs = list(jobs.find({ "node": node["_id"], "status": { "$nin": ["COMPLETED", "FAILED"] }}))
        if len(node_jobs) > 0:
            for job in node_jobs:
                update_status(job["containerId"], job["_id"], "NODE_STARTED")
            time.sleep(0.5)
            for job in node_jobs:
                update_status(job["containerId"], job["_id"], "PENDING")
        return node["_id"]
    return None


# Update node status in MongoDB and de-provision EC2 instance
def shutdown_node(nodeId):
    col = db["nodes"]

    timestamp = datetime.now(timezone.utc)

    col.update_one({ "_id": nodeId }, { "$set": {
        "alive": False,
        "killed": timestamp, 
    }})
    
    ec2.terminate_instances(InstanceIds=[instanceId])


# Dynamically generate Dockerfile for passed job
def generate_dockerfile(containerId):

    def getPath(folder, container):
        path = []
        if folder != "~":
            curFolder = container["folders"][folder]
            if not curFolder:
                return -1
            else:
                while True:
                    path.insert(0, curFolder["name"])
                    if curFolder["parent"] == "~":
                        break
                    curFolder = container["folders"][curFolder["parent"]]
        return f"{'/'.join(path)}{'/' if len(path) > 0 else ''}"

    containers = db["containers"]
    files = db["files"]
 
    containerId = containerId if isinstance(containerId, ObjectId) else ObjectId(containerId)
    
    container = containers.find_one({ "_id": containerId })
    entryPoint = files.find_one({ "_id": container["entryPoint"] })

    parts = entryPoint["name"].split(".")
    
    templatePath = f"/opt/cloudcontain-job-execution/{parts[1].lower()}.dockerfile"
    with open(templatePath, "r") as templateFile:
        template = templateFile.read()

    content = template.replace("{{ENTRY_POINT_FILENAME}}", parts[0])
    content = content.replace("{{ENTRY_POINT_FILE}}", entryPoint["name"])
    content = content.replace("{{ENTRY_POINT_PATH}}", getPath(entryPoint["folder"], container) + entryPoint["name"])

    outputPath = os.path.join(f"/tmp/cloudcontain-jobs/{job['containerId']}", "Dockerfile")
    with open(outputPath, "w") as output:
        output.write(content)


# Register instance and listen for jobs
if __name__ == "__main__":
    nodeId = register_node()
    lastActivity = int(time.time()) 
    while True:

        now = int(time.time())
        if now - lastActivity >= 600:
            shutdown_node(nodeId)
            break

        try:
            response = sqs.receive_message(
                    QueueUrl=SQS_URL,
                    MaxNumberOfMessages=1,
                    WaitTimeSeconds=20
            )

            jobRequest = response.get('Messages', [])

            # No requests received, continue polling
            if not jobRequest:
                continue

            # Job request received, begin processing
            jobRequest = jobRequest[0]
            lastActivity = int(time.time())

            # Load job info payload
            job = json.loads(jobRequest['Body'])

            # Notify Pusher build is starting
            update_status(job["containerId"], job["jobId"], "STARTED")
            time.sleep(0.5)

            # Begin cloning container files
            update_status(job["containerId"], job["jobId"], "CLONING")
            subprocess.run(["aws", "s3", "sync", f"s3://cloudcontain-containers/{job['containerId']}/project", f"/tmp/cloudcontain-jobs/{job['containerId']}"]) 
            time.sleep(0.5)

            # Begin docker build of container files
            update_status(job["containerId"], job["jobId"], "CONTAINERIZING")
            generate_dockerfile(job["containerId"])
            buildProcess = subprocess.Popen(["docker", "build", "-t", f"job-{job['jobId']}", f"/tmp/cloudcontain-jobs/{job['containerId']}"], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, universal_newlines=True, bufsize=1) 
            
            try:
                for line in buildProcess.stdout:
                    pusher_client.trigger(job["containerId"], 'job-output', {
                        "jobId": job["jobId"],
                        "content": line,
                        "timestamp": str(datetime.now(timezone.utc)),
                        "build": True
                    })
            finally:
                buildProcess.stdout.close()
                buildCode = buildProcess.wait()
                time.sleep(0.5)

                # Check if build failed
                if buildCode != 0:
                    update_status(job["containerId"], job["jobId"], "BUILD_FAILED")
                    subprocess.run(["rm", "-rf", f"/tmp/cloudcontain-jobs/{job['containerId']}"])
                    
                    sqs.delete_message(
                        QueueUrl=SQS_URL,
                        ReceiptHandle=jobRequest['ReceiptHandle']
                    )

                    continue
                
                # Begin docker run of container files
                update_status(job["containerId"], job["jobId"], "RUNNING")
                jobProcess = subprocess.Popen(["docker", "run", "--rm", "--memory=512m", "--cpus=1", "--name", f"job-{job['jobId']}", f"job-{job['jobId']}"], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, universal_newlines=True, bufsize=1)

                try:
                    for line in jobProcess.stdout:
                        pusher_client.trigger(job["containerId"], 'job-output', {
                            "jobId": job["jobId"],
                            "content": line,
                            "timestamp": str(datetime.now(timezone.utc)),
                            "build": False
                        })
                finally:
                    jobProcess.stdout.close()
                    exitCode = jobProcess.wait()
                    time.sleep(0.5)

                    # Clean up tmp files and remove docker image
                    update_status(job["containerId"], job["jobId"], "CLEANING")
                    subprocess.run(["rm", "-rf", f"/tmp/cloudcontain-jobs/{job['containerId']}"])
                    subprocess.run(["sudo", "docker", "rmi", f"job-{job['jobId']}"])
                    time.sleep(0.5)

                    # Notify Pusher of success/failure
                    update_status(job["containerId"], job["jobId"], "COMPLETED" if exitCode == 0 else "FAILED")

                    # Remove queue item once execution is completed
                    sqs.delete_message(
                        QueueUrl=SQS_URL,
                        ReceiptHandle=jobRequest['ReceiptHandle']
                    )

        except Exception as e:
            print(f"Error: {e}")
