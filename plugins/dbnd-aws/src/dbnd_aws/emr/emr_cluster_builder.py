class EmrClusterBuilder(object):
    def __init__(self, emr_client, ec2_client):
        super(EmrClusterBuilder, self).__init__()
        self.emr = emr_client
        self.ec2 = ec2_client  # boto3.client("ec2", region_name=region_name)

    def get_security_group_id(self, group_name, region_name):
        response = self.ec2.describe_security_groups(GroupNames=[group_name])
        return response["SecurityGroups"][0]["GroupId"]

    def create_cluster(
        self,
        region_name,
        cluster_name,
        release_label="emr-5.16.0",
        master_instance_type="m3.xlarge",
        num_core_nodes=2,
        core_node_instance_type="m3.2xlarge",
    ):
        emr_master_security_group_id = self.get_security_group_id(
            "ElasticMapReduce-master", region_name=region_name
        )
        emr_slave_security_group_id = self.get_security_group_id(
            "ElasticMapReduce-slave", region_name=region_name
        )
        cluster_response = self.emr.run_job_flow(
            Name=cluster_name,
            ReleaseLabel=release_label,
            Instances={
                "InstanceGroups": [
                    {
                        "Name": "Master nodes",
                        "Market": "ON_DEMAND",
                        "InstanceRole": "MASTER",
                        "InstanceType": master_instance_type,
                        "InstanceCount": 1,
                    },
                    {
                        "Name": "Slave nodes",
                        "Market": "ON_DEMAND",
                        "InstanceRole": "CORE",
                        "InstanceType": core_node_instance_type,
                        "InstanceCount": num_core_nodes,
                    },
                ],
                "KeepJobFlowAliveWhenNoSteps": True,
                "Ec2KeyName": "databand-dev",
                "EmrManagedMasterSecurityGroup": emr_master_security_group_id,
                "EmrManagedSlaveSecurityGroup": emr_slave_security_group_id,
            },
            VisibleToAllUsers=True,
            JobFlowRole="EMR_EC2_DefaultRole",
            ServiceRole="EMR_DefaultRole",
            Applications=[
                {"Name": "hadoop"},
                {"Name": "spark"},
                {"Name": "hive"},
                {"Name": "livy"},
                {"Name": "zeppelin"},
            ],
        )
        return cluster_response["JobFlowId"]
