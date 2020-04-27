import configparser
import json
import sys
import time

import boto3
import pandas as pd
import psycopg2




def create_iam_role(iam, DWH_IAM_ROLE_NAME):
    try:
        print('1.1 Creating a new IAM Role')
        dwhRole = iam.create_role(
                Path='/',
                RoleName=DWH_IAM_ROLE_NAME,
                Description="Allows Redshift clusters to call AWS services on your behalf.",
                AssumeRolePolicyDocument=json.dumps(
                    {'Statement': [{'Action': 'sts:AssumeRole',
                                   'Effect': 'Allow',
                                   'Principal': {'Service': 'redshift.amazonaws.com'}}],
                    'Version': '2012-10-17'})
        )
        
        print('1.2 Attaching Policy')
        iam.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME,
                              PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")['ResponseMetadata']['HTTPStatusCode']

        print('1.3 Get the IAM role ARN')
        roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']

        return roleArn

    except Exception as e:
        print(e)

    
    
    
def create_cluster(redshift, DWH_CLUSTER_TYPE, DWH_NODE_TYPE, DWH_NUM_NODES, DWH_DB,
                   DWH_CLUSTER_IDENTIFIER, DWH_DB_USER, DWH_DB_PASSWORD, roleArn):
    try:
        print("\nCreating redshift cluster...")
        response = redshift.create_cluster(        
            ClusterType=DWH_CLUSTER_TYPE,
            NodeType=DWH_NODE_TYPE,
            NumberOfNodes=int(DWH_NUM_NODES),

            DBName=DWH_DB,
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
            MasterUsername=DWH_DB_USER,
            MasterUserPassword=DWH_DB_PASSWORD,

            IamRoles=[roleArn]
        )
        
    except Exception as e:
        print(e)




def open_port(ec2, myClusterProps, DWH_PORT):
    try:
        vpc = ec2.Vpc(id=myClusterProps['VpcId'])
        defaultSg = list(vpc.security_groups.all())[0]

        defaultSg.authorize_ingress(
            GroupName= defaultSg.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(DWH_PORT),
            ToPort=int(DWH_PORT)
        )
    except Exception as e:
        print(e)
        
    
    
    
def main():
    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))
    
    KEY                    = config.get('AWS','KEY')
    SECRET                 = config.get('AWS','SECRET')

    DWH_CLUSTER_TYPE       = config.get("DWH","DWH_CLUSTER_TYPE")
    DWH_NUM_NODES          = config.get("DWH","DWH_NUM_NODES")
    DWH_NODE_TYPE          = config.get("DWH","DWH_NODE_TYPE")

    DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
    DWH_DB                 = config.get("DWH","DWH_DB")
    DWH_DB_USER            = config.get("DWH","DWH_DB_USER")
    DWH_DB_PASSWORD        = config.get("DWH","DWH_DB_PASSWORD")
    DWH_PORT               = config.get("DWH","DWH_PORT")

    DWH_IAM_ROLE_NAME      = config.get("DWH", "DWH_IAM_ROLE_NAME")

    pd.DataFrame({"Param":
                  ["DWH_CLUSTER_TYPE", "DWH_NUM_NODES", "DWH_NODE_TYPE", "DWH_CLUSTER_IDENTIFIER", "DWH_DB", "DWH_DB_USER", "DWH_DB_PASSWORD",
                   "DWH_PORT", "DWH_IAM_ROLE_NAME"],
                  "Value":
                  [DWH_CLUSTER_TYPE, DWH_NUM_NODES, DWH_NODE_TYPE, DWH_CLUSTER_IDENTIFIER, DWH_DB, DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT,
                   DWH_IAM_ROLE_NAME]
                 })
    
    ec2 = boto3.resource('ec2',
                         region_name="us-west-2",
                         aws_access_key_id=KEY,
                         aws_secret_access_key=SECRET)

    s3 = boto3.resource('s3',
                        region_name="us-west-2",
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET)

    iam = boto3.client('iam',
                       region_name="us-west-2",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET)

    redshift = boto3.client('redshift',
                            region_name="us-west-2",
                            aws_access_key_id=KEY,
                            aws_secret_access_key=SECRET)
    
    roleArn = create_iam_role(iam, DWH_IAM_ROLE_NAME)
    
    create_cluster(redshift, DWH_CLUSTER_TYPE, DWH_NODE_TYPE, DWH_NUM_NODES, DWH_DB,
                   DWH_CLUSTER_IDENTIFIER, DWH_DB_USER, DWH_DB_PASSWORD, roleArn)
    
    myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    
    while myClusterProps['ClusterStatus'] != 'available':
        time.sleep(1) # avoid throttling rate exeeded error
        myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    
    print("Redshift cluster successfully created.")
    DWH_ENDPOINT = myClusterProps['Endpoint']['Address']
    print('Endpoint: ', DWH_ENDPOINT)
    DWH_ROLE_ARN = myClusterProps['IamRoles'][0]['IamRoleArn']
    print('ARN: ', DWH_ROLE_ARN)
    
    open_port(ec2, myClusterProps, DWH_PORT)
    
    config.set('CLUSTER', 'HOST', DWH_ENDPOINT)
    config.set('CLUSTER', 'DB_NAME', DWH_DB)
    config.set('CLUSTER', 'DB_USER', DWH_DB_USER)
    config.set('CLUSTER', 'DB_PASSWORD', DWH_DB_PASSWORD)
    config.set('CLUSTER', 'DB_PORT', DWH_PORT)
    
    config.set('IAM_ROLE', 'ARN', roleArn)
    config.write(sys.stdout)
    
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    print('Connected')

    conn.close()
    
    
    
    
if __name__=="__main__":
    main()