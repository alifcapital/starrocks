#!/usr/bin/env bash
# Run on the workstation after saving validation receipts and stopping the cluster.
set -euo pipefail
instance=${1:?Usage: bake-ami.sh INSTANCE_ID AMI_NAME}
name=${2:?Usage: bake-ami.sh INSTANCE_ID AMI_NAME}
region=${AWS_REGION:-eu-central-1}
aws ec2 stop-instances --region "$region" --instance-ids "$instance" >&2
aws ec2 wait instance-stopped --region "$region" --instance-ids "$instance"
image=$(aws ec2 create-image --region "$region" --instance-id "$instance" --name "$name" \
    --description 'StarRocks 4.1 builder: Ubuntu 24.04, JDK 21, GCC 14.3; see GOLDEN-4.1-UBUNTU24.md in the image' \
    --tag-specifications "ResourceType=image,Tags=[{Key=Name,Value=$name}]" \
    --query ImageId --output text)
printf '%s\n' "$image"
echo "Waiting for $image to become available; donor stays stopped" >&2
aws ec2 wait image-available --region "$region" --image-ids "$image"
aws ec2 describe-images --region "$region" --image-ids "$image" \
    --query 'Images[].{ImageId:ImageId,State:State,Name:Name,BlockDeviceMappings:BlockDeviceMappings}' >&2
