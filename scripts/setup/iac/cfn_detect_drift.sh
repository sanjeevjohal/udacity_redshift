#!/bin/zsh

# Prompt user to select an AWS profile
echo "Select an AWS profile:"
select profile in $(grep '\[profile' ~/.aws/config | awk '{print $2}' | tr -d ']'); do
  break
done

echo "Selected profile: $profile"

# Prompt user to enter the stack name
#echo "Enter the stack name:"
#read stack_name
#if [ -z "$stack_name" ]; then
#  echo "No stack name entered. Choose from this list." >&2
#  select stack_name in SJ-Udacity-Mod2-Redshift hiraku-slo-nonprod; do
#    break
#  done
#fi

echo "Choose from this list of stacks"
select stack_name in SJ-Udacity-Mod2-Redshift hiraku-slo-nonprod hiraku-slo-prod; do
    break
  done

## TODO: add error handling for invalid stack name or choose from list of stacks using iac tags (owner: Sanjeev Johal)
#iac cloudformation list-stacks --profile $profile --query "StackSummaries[?Tags[?Key=='owner' && Value=='Sanjeev Johal']].StackName" --output text
echo "Selected stack: $stack_name"

# Detect stack drift
aws cloudformation detect-stack-drift --profile $profile --stack-name $stack_name --query 'StackDriftDetectionId' --output text > detection-id.txt

# wait for drift detection to complete
while true; do
    driftStatus=$(aws cloudformation describe-stack-drift-detection-status --profile $profile --stack-drift-detection-id $(cat detection-id.txt) --query 'DetectionStatus' --output text)
    if [[ $driftStatus == "DETECTION_IN_PROGRESS" ]]; then
      echo "Stack drift detection in progress..."
      sleep 5
    else
      echo "Stack drift detection complete."
      aws cloudformation describe-stack-resource-drifts --profile $profile --stack-name $stack_name --query 'StackResourceDrifts[].{Resource:LogicalResourceId,DriftStatus:StackResourceDriftStatus}' --output table
      break
    fi
  done

