---
"title": "Submitting a Kubernetes Run from a Local Machine"
---
**Requirements: **

1. You need to be able to submit POD to Kubernetes from your local machine.
2. You need to be able to push a Docker image from your local machine to the repository used by the Kubernetes cluster.
3. You need to be able to create a namespace at Kubernetes or reuse an existing one.
4. POD need to have access to Airflow DB and Databand service.
5. The local machine needs to have access to the Databand service (for debugging Airflow, DB should be available, too).