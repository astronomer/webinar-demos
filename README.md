# Airflow 3.0 Security Enhancements: Remote Execution and Task Isolation Deep Dive

This is the companion repository for the [Airflow 3.0 Security Enhancements: Remote Execution and Task Isolation Deep Dive webinar](https://www.astronomer.io/events/webinars/apache-airflow-3-stronger-security-and-remote-execution-video). It explains how to use the Astro feature [Remote Execution Agents](https://www.astronomer.io/docs/astro/remote-execution-agents) with the Agents running on AWS EKS.

If you are not an Astronomer customer, you can implement Remote Execution using the Edge Executor of the [Edge3 provider package](https://airflow.apache.org/docs/apache-airflow-providers-edge3/stable/index.html).

## How to set up Remote Execution Agents on AWS EKS

Note: It is a good best practice to have all resources in the same region.

### Setup on Astro

1. Make sure you have a dedicated Astro Cluster in AWS. Create a new one if you don't have one yet. YOu can leave the VPC Subnet range at its default setting (`172.20.0.0/19`) or customize it for your needs. Note that it can take up to an hour for a new cluster to be provisioned.

2. Create a new Astro Deployment with the following details:

    - Mode: Remote Execution
    - Cluster: your dedicated AWS cluster
    - Executor: Astro Executor
    - Allowed IP Address Ranges: Set depending on your needs, you can also leave it empty for now to allow the full range of IP addresses and/or set a range later.

![Screenshot of the Astro UI showing provisioning of a Remote Execution Deployment](/src/img/remote_execution_deployment.png)

3. Go to the **Remote Agents** tab and select **Tokens** to get a new Agent Token. This is the secret the Agents use to authenticate themselves to the Astro deployment, when sending their heartbeats and requests to the API server. Make sure to safe the token in a secure place, you'll need it later!

![Screenshot of the Astro UI showing how to get an Agent Token](/src/img/get_agent_token.png)

4. Next, click on **Agents** and **Register a Remote Agent**. You'll be given a `values.yaml` file to download and instructions on how to install the helm chart. Download the `values.yaml` file. It looks similar to [this file](/remote_execution_eks_templates/values.yaml) but it will contain your `astroDeploymentAPIURL` and your `namespace` instead of a placeholder. Depending on your Astro Runtime version the images for the agents will also differ.

5. You will also need a Deployment level API Token, so the Agents are able to fetch their images. Go to the **Access** tab and click on **API Tokens** then create a new Deployment API Token. Make sure you save it in a secure location.

![Screenshot of the Astro UI showing how to get a Deployment API Token](/src/img/get_deployment_api_token.png)

### Setup EKS

6. Makes sure you have the [aws cli](https://docs.aws.amazon.com/cli/latest/) and the [eksctl](https://github.com/eksctl-io/eksctl) installed.

7. Create your EKS cluster, make sure that the `workers` node group is large enough to support your intended workloads and the Agent specifications in your values.yaml for all 3 Agents. See the [template Airflow cluster file](/remote_execution_eks_templates/my-airflow-cluster.yaml) for an example.

To create your cluster based on the file run the following eksctl command. This can take up to 15-25min.

    ```bash
    eksctl create cluster -f my-airflow-cluster.yaml
    ```

8. Configure kubectl to connect to your newly created EKS cluster:

    ```bash
    aws eks update-kubeconfig --region us-east-1 --name remote-execution-airflow-cluster
    ```

    Verify the connection works:

    ```bash
    kubectl get nodes
    ```

9. Next, you are going to need to prepare your cluster to be ready to install the Agent based on the Astronomer Helm chart. It needs:

    - A namespace. In this example `my-namespace`.

    ```bash
    kubectl create namespace my-namespace
    ```

    - That namespace needs to contain the Agent Token retrieved in step 3 as a secret. In this example the secret will be called `my-agent-token`.

    ```bash
    kubectl create secret generic my-agent-token --from-literal=token=<your-agent-token> -n my-namespace
    ```

    - The second secret needed is the Deployment token retrieved in step 5 which will be used to pull the images for the Agents. In this example the secret will be called `my-astro-registry-secret`.

    ```bash
    kubectl create secret docker-registry my-astro-registry-secret \
        --namespace my-namespace \
        --docker-server=images.astronomer.cloud \
        --docker-username=cli \
        --docker-password=<your_astro_token>
    ```

    Note that it is also possible to add the secrets directly to the values.yaml file and enable auto-creation of the namespace.


10. Now it is time to modify values.yaml!

    - `resourceNamePrefix` : Add any prefix you like
    - `namespace` : Add the namespace you just created in EKS. Here `my-namespace`.
    - `imagePullSecretName`: Add the name of the secret that contains the Deployment token. Here `my-astro-registry-secret`.
    - `agentTokenSecretName`: Add the name of the secret that contains the Agent token. Here `my-agent-token`.
    - `secretBackend` : Add the class to your secrets backend. If you don't have one (yet) add `airflow.secrets.local_filesystem.LocalFilesystemBackend`. If you use Airflow connections in your DAGs they will need to be stored in this backend.
    - `xcomBackend`: Since the workers are remote they can't use the Airflow metadata database as an XCom backend and need a custom one. For this example we'll set up a custom XCom Backend in Object storage with `airflow.providers.common.io.xcom.backend.XComObjectStorageBackend`. We'll give credentials to the worker to use this XCom backend in a later step.
    - `dagBundleConfigList`: This config variable needs to be set to contain at least one DAG bundle that is accessible to the worker. We'll use a GitDagBundle. The credentials for this will be in the Airflow connection defined with the provided `git_conn_id`. For example: `'[{"name": "gitbundle-1", "classpath": "airflow.providers.git.bundles.git.GitDagBundle", "kwargs": {"git_conn_id": "git_default", "subdir": "dags", "tracking_ref": "main", "refresh_interval": 10}}]'` 

    This DAG bundle uses the `git_default` connection, which also contains the repo name. And checks for DAGs in the `dags` folder on the `main` branch. It checks for changes every 10 seconds (`refresh_interval`).
    
    To learn more about DAG bundles see the [DAG versioning and DAG bundles](https://www.astronomer.io/docs/learn/airflow-dag-versioning/) guide.

    - `commonEnv`: This needs to contain a few environment valriables to enable the Agents to use remote the GitDagBundle, remote logging in S3 and sending XCom to S3. The variables needed are:

        ```yaml
        - name: ASTRONOMER_ENVIRONMENT
          value: "cloud"

        # This is the connection used in the GitDagBundle. If you want to access a private repo you need an access token with read and write permissions.
        - name: AIRFLOW_CONN_GIT_DEFAULT
          value: '{"conn_type": "git", "login": "<your GH login>", "password": "<access_token>", "host": "https://github.com/<account>/<repo>"}'

        # The next set of variables are needed for remote logging to S3
        - name: AIRFLOW__LOGGING__REMOTE_LOGGING
          value: True
        - name: AIRFLOW__LOGGING__LOGGING_CONFIG_CLASS
          value: "astronomer.runtime.logging.logging_config"
        - name: AIRFLOW__LOGGING__REMOTE_BASE_LOG_FOLDER
          value: s3://<your-bucket>/logs   # replace with your bucket!
        - name: AIRFLOW__LOGGING__REMOTE_LOG_CONN_ID
          value: AWS_DEFAULT
        # region containing your remote logging bucket, only necessary if it is a different region than your EKS cluster
        - name: AWS_DEFAULT_REGION
          value: "<your-region>"
        - name: ASTRO_LOGGING_AWS_WEB_IDENTITY_TOKEN_FILE
          value: "/tmp/logging-token"

        # This Airflow connection is of course incomplete, the credentials will be fetched using a role attached to the worker service account set up in a later step
        - name: AIRFLOW_CONN_AWS_DEFAULT  
          value: '{"conn_type": "aws", "extra": {"region_name": "<your-region>"}}'

        # These two environment variables are needed for the custom XCom backend
        - name: AIRFLOW__COMMON_IO__XCOM_OBJECTSTORAGE_PATH
          value: "s3://aws_default@<your-bucket>/xcom"  # replace the bucket with your XCom bucket. Uses the same aws_default connection
        - name: AIRFLOW__COMMON_IO__XCOM_OBJECTSTORAGE_THRESHOLD
          value: "0"  # all XCom will be stored in Object storage
        ```

11. Add the Astronomer Helm repo, update it and then install the helm chart for the Agents in your cluster:

    ```bash
    helm repo add astronomer https://helm.astronomer.io/
    helm repo update
    helm install astro-agent astronomer/astro-remote-execution-agent --namespace my-namespace --values values.yaml
    ```

12. After a few minutes you should see the Agents heart-beating in your Astro deployment.

![Screenshot of an Astro deployment showing 3 Agents](/src/img/three-happy-agents.png)

    You can also check in on your pods using the following commands:

    ```bash
    kubectl get pods -n my-namespace
    kubectl describe pods -n my-namespace
    ```

13. Now you should see and be able to run any DAGs stored in all registered DAG bundles! You will noticed that 2 things are missing: Tasks that push to or pull from XCom still fail and there are no task logs displayed yet. This is because the worker pod needs access to S3 to push and pull XCom and store its task logs. And the Astro deployment needs the ability to read the task logs in S3. Let's fix that!

### Finish setting up the custom XCom backend

You already set the necessary environment variables in `values.yaml` fot use the `XComObjectStorageBackend`. 

```yaml
xcomBackend: "airflow.providers.common.io.xcom.backend.XComObjectStorageBackend"
```

and

```yaml
commonEnv:
  # ...
  - name: AIRFLOW_CONN_AWS_DEFAULT  
    value: '{"conn_type": "aws", "extra": {"region_name": "us-east-1"}}'

  - name: AIRFLOW__COMMON_IO__XCOM_OBJECTSTORAGE_PATH
    value: "s3://aws_default@<your bucket>/xcom"
  - name: AIRFLOW__COMMON_IO__XCOM_OBJECTSTORAGE_THRESHOLD
    value: "0"
```

#### Install the providers on the worker

In order for the worker pod to be able to use the `XComObjectStorageBackend` with S3 it needs to have the Airflow Amazon provider (with the s3fs extra) and the Airflow Common IO provider installed. To speed up this process you can use a constraints file, like the [example provided](/remote_execution_eks_templates/constraints.txt).

12. Add the constraints file as a configmap to the k8s cluster. Update the versions as needed.

    ```bash
    kubectl create configmap constraints-configmap --from-file=constraints.txt -n my-namespace
    ```

13. Update the values.yaml workers initContainers to install the 2 necessary providers sing the constraints file from the config map. Update the versions as needed. Also make sure to add the shared packages to the PYTHONPATH.

```yaml
workers:
  initContainers:
    - name: install-amazon-provider-s3fs
      image: images.astronomer.cloud/baseimages/astro-remote-execution-agent:3.0-4-python-3.12-astro-agent-1.0.2
      command: ["pip", "install", "--target", "/shared/packages", "--prefer-binary","--constraint", "/constraints/constraints.txt",
        "apache-airflow-providers-amazon[s3fs]==9.9.0", 
        "apache-airflow-providers-common-io==1.6.1"]
      volumeMounts:
        - name: shared-packages
          mountPath: /shared/packages
        - name: constraints
          mountPath: /constraints


  env:
    - name: PYTHONPATH
      value: "/shared/packages:$PYTHONPATH"
```

and add the related volumen mounts

```yaml
    volumes:
      - name: shared-packages
        emptyDir: {}
      - name: constraints
        configMap:
          name: constraints-configmap

    # The mount information for the volumes to be mounted in the Agent Worker container
    # This is useful to mount any external volumes in the Agent Worker container
    # For example, one can use this field to mount a shared volume for the Agent Worker container
    # to ship the task logs to a logging backend via the logging sidecar container
    # Example:
    # volumeMounts:
    #   - name: task-logs
    #     mountPath: /var/log/airflow
    #     readOnly: true
    volumeMounts:
      - name: shared-packages
        mountPath: /shared/packages
        readOnly: true
```

#### AWS Permissions for the worker

Now the only thing left is to authenticate the worker pod to S3.

For this you need to set up a policy and a role, then add that role to the worker service account.

14. If you don't have one already, create a new S3 bucket

15. Create your AirflowS3Access policy using the template at [my-airflow-s3-policy.json](/remote_execution_eks_templates/my-airflow-s3-policy.json). Note that in this example we are storing the logs and XCom in the same bucket, you can also add several buckets to this policy to store logs and xcom separatedly.

    ```
    aws iam create-policy \
    --policy-name AirflowS3Access \
    --policy-document file://my-airflow-s3-policy.json
    ```

    Make sure to record the ARN from the CLI output: `arn:aws:iam::<your-acccoun-id>:policy/AirflowS3Access`

16. Now you need an IAm role to attach this policy to. The IAM role's trust policy needs to include the EKS OIDC ID. So you need to fetch that first. 


    ```bash
    OIDC_ISSUER_URL=$(aws eks describe-cluster --name <YOUR_EKS_CLUSTER_NAME> --query "cluster.identity.oidc.issuer" --output text)
    EKS_OIDC_ID=$(echo "$OIDC_ISSUER_URL" | sed -e 's|https://oidc.eks.<YOUR_AWS_REGION>.amazonaws.com/id/||')
    echo $EKS_OIDC_ID
    ```

    The output looks something like this: 
    `https://oidc.eks.us-east-1.amazonaws.com/id/<your cluster id>`

    Add the cluster ID and your AWS account ID for the placeholders in [my-airflow-s3-role-trust-policy.json](/remote_execution_eks_templates/my-airflow-s3-role-trust-policy.json). Also make sure to double check the regions!

17. Create a new role with that trust policy.

    ```bash
    aws iam create-role \
    --role-name AirflowS3LoggerXComRole \
    --assume-role-policy-document file://my-airflow-s3-role-trust-policy.json
    ```

5. Attach the permissions policy to the role:

    ```bash
    aws iam attach-role-policy \
    --role-name AirflowS3LoggerXComRole \
    --policy-arn arn:aws:iam::<YOUR_AWS_ACCOUNT_ID>:policy/AirflowS3Access
    ```

    Record the ARN of the role from the output!

18. Annotate the role to your pods in the `values.yaml`

    ```yaml
    annotations:
      eks.amazonaws.com/role-arn: "arn:aws:iam::001177193081:role/AirflowS3LoggerXComRole"
    ```

    Update the helm with the new values.yaml with:

    ```bash
    helm upgrade astro-agent astronomer/astro-remote-execution-agent --namespace my-namespace --values values.yaml
    ```

19. Run a DAG, you should now be able to pass data between tasks via XCom and see the logs appear in your logging location!


### Customer Managed workload identity to read logs

20. On the Astro deployment go to the Details tab and click on **Edit** on the Advanced section.

![Astro UI Screenshot Details tab](/src/img/configure-task-logs-1.png)

21. Select **Bucket Storage** for task logs and provide the URL where your logs are stored (the same as `AIRFLOW__LOGGING__REMOTE_BASE_LOG_FOLDER` in values.yaml).

22. If you have not yet for your cluster, you'll need to create a customer managed workload identity. In the pop up, enter the AWS role that you created with S3 access, or if you prefer a separate AWS role that has at least read permissions for the logs location. In this example it would be `arn:aws:iam::001177193081:role/AirflowS3LoggerXComRole`. Run the provided command in your terminal.

23. You should now see the logs in your Airflow environment!

