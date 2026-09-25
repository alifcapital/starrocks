#!/usr/bin/env bash
# Run on a workstation with AWS credentials and SSH access to the build subnet.
set -euo pipefail
region=${AWS_REGION:-eu-central-1}
instance_type=${SR_INSTANCE_TYPE:-m6i.4xlarge}
image=${SR_AMI_ID:-}
if [[ -z $image ]]; then
    image=$(aws ssm get-parameter --region "$region" \
        --name /aws/service/canonical/ubuntu/server/24.04/stable/current/amd64/hvm/ebs-gp3/ami-id \
        --query Parameter.Value --output text)
fi
args=(aws ec2 run-instances --region "$region" --image-id "$image"
    --instance-type "$instance_type" --key-name 'eshishkin alif'
    --subnet-id subnet-000e5b6e91c139dda
    --security-group-ids sg-0b4d15c7902740ca5 sg-0b03af9a1a60acbe6
    --iam-instance-profile Name=starrocks-role --disable-api-termination
    --metadata-options HttpTokens=required,HttpPutResponseHopLimit=2
    --block-device-mappings '[{"DeviceName":"/dev/sda1","Ebs":{"VolumeSize":300,"VolumeType":"gp3","Iops":6000,"Throughput":500,"DeleteOnTermination":true}}]'
    --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=${SR_INSTANCE_NAME:-starrocks-4.1-ubuntu24}}]")
if [[ -z ${SR_AMI_ID:-} ]]; then
    args+=(--user-data "file://$(cd "$(dirname "$0")" && pwd)/prepare-host.sh")
fi
"${args[@]}"
