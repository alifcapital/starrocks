#!/bin/bash

# Script to create a new AMI from an EC2 instance and update a launch template
# This script prompts for the AMI name but uses hardcoded instance ID and launch template ID

set -e

# Check if AWS CLI is installed
if ! command -v aws &> /dev/null; then
    echo "AWS CLI is not installed. Please install it first."
    exit 1
fi

# Check AWS CLI configuration
if ! aws sts get-caller-identity &> /dev/null; then
    echo "AWS CLI is not configured. Please run 'aws configure' first."
    exit 1
fi

# Hardcoded values
INSTANCE_ID="i-0e0661abf4a6f061e"
LAUNCH_TEMPLATE_ID="lt-04845c7ce14f901dd"
LAUNCH_TEMPLATE_VERSION='$Latest'

# Default AMI name with timestamp
DEFAULT_AMI_NAME="AMI-$(date +%Y%m%d-%H%M%S)"

# Ask for AMI name
read -p "Enter AMI name [default: $DEFAULT_AMI_NAME]: " AMI_NAME
AMI_NAME=${AMI_NAME:-$DEFAULT_AMI_NAME}

echo "Creating AMI from instance $INSTANCE_ID with name $AMI_NAME..."

# Create AMI from the instance
AMI_ID=$(aws ec2 create-image \
    --instance-id "$INSTANCE_ID" \
    --name "$AMI_NAME" \
    --description "AMI created from $INSTANCE_ID on $(date)" \
    --no-reboot \
    --query 'ImageId' \
    --output text)

echo "New AMI created: $AMI_ID"
echo "Waiting for AMI to become available (this may take several minutes)..."

# Wait for the AMI to be available
aws ec2 wait image-available --image-ids "$AMI_ID"

echo "AMI $AMI_ID is now available"

# Get the current launch template version
echo "Getting latest version of launch template $LAUNCH_TEMPLATE_ID..."
TEMPLATE_DATA=$(aws ec2 describe-launch-template-versions \
    --launch-template-id "$LAUNCH_TEMPLATE_ID" \
    --versions "$LAUNCH_TEMPLATE_VERSION" \
    --query 'LaunchTemplateVersions[0].LaunchTemplateData')

# Update the launch template with the new AMI ID
echo "Updating launch template $LAUNCH_TEMPLATE_ID with new AMI $AMI_ID..."
aws ec2 create-launch-template-version \
    --launch-template-id "$LAUNCH_TEMPLATE_ID" \
    --version-description "Updated with AMI $AMI_ID" \
    --source-version "$LAUNCH_TEMPLATE_VERSION" \
    --launch-template-data "{\"ImageId\":\"$AMI_ID\"}"

echo "Setting new version as default..."
aws ec2 modify-launch-template \
    --launch-template-id "$LAUNCH_TEMPLATE_ID" \
    --default-version '$Latest'

echo "Launch template $LAUNCH_TEMPLATE_ID has been updated with AMI $AMI_ID"

# Verify the update
CURRENT_AMI=$(aws ec2 describe-launch-template-versions \
    --launch-template-id "$LAUNCH_TEMPLATE_ID" \
    --versions '$Latest' \
    --query 'LaunchTemplateVersions[0].LaunchTemplateData.ImageId' \
    --output text)

echo "Verification: Launch template is now using AMI $CURRENT_AMI"
