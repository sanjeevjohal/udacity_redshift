#!/usr/bin/env zsh
echo ">>> START <<<\n"

# Prompt user to select an AWS profile
echo "Select an AWS profile:"
select AWS_PROFILE in $(grep '\[profile' ~/.aws/config | awk '{print $2}' | tr -d ']'); do
  break
done

echo "Selected profile: $AWS_PROFILE"


AWS_ACCOUNT=$(aws sts get-caller-identity --output text --query Account --profile ${AWS_PROFILE})
echo "${AWS_PROFILE} \ ${AWS_ACCOUNT}\n"

# functions #
get_caller_identity() {
  cmd=$(aws sts get-caller-identity --profile $AWS_PROFILE)
}

describe_stack() {
  local _sn=$1
  stack=$(aws cloudformation describe-stacks \
    --stack-name $_sn \
    --output table \
    --profile $AWS_PROFILE)
  echo "${stack}"
}

does_stack_exist() {
  local _sn=$1
  if stack_status=$(aws cloudformation describe-stacks \
      --stack-name $_sn \
      --profile $AWS_PROFILE \
      --query "Stacks[0].StackStatus" \
      --output text); then
    echo $stack_status
    return 0
  else
    echo "Stack does not exist"
    return 1
  fi
}

do_create_stack() {
  local stackName=$1
  local templateBody=$2

#  if stack already exists and status is rollback_complete or rollback_failed, delete stack
  if does_stack_exist $stackName; then
    stackStatus=$(does_stack_exist $stackName)
    if [[ $stackStatus == "ROLLBACK_COMPLETE" || $stackStatus == "ROLLBACK_FAILED" ]]; then
      echo "Stack $stackName already exists and is in $stackStatus status. Deleting stack..."
      do_delete_stack $stackName
    else
      echo "Stack $stackName already exists and is in $stackStatus status. Skipping stack creation..."
      return 1
      exit
    fi
  fi

  echo ">>> Creating stack $stackName"
  cmd=$(aws cloudformation create-stack \
        --stack-name $stackName \
        --template-body file://../cfn/$templateBody \
        --capabilities CAPABILITY_NAMED_IAM \
        --profile $AWS_PROFILE 2>&1
        )

  if [[ $? -ne 0 ]]; then
    echo "Error creating stack: $cmd"
    return 1
  fi

  start_time=$(date +%s) # get current time
  retries=20
  while ((retries > 0)); do
    stackStatus=$(does_stack_exist $stackName)
    echo "Stack Status: $stackStatus"
    echo "Elapsed time: $(($(date +%s) - $start_time)) seconds"
    echo "Retries left: $retries"
    case $stackStatus in
      CREATE_COMPLETE|UPDATE_COMPLETE)
        echo "*** Stack creation complete ***"
        return 0
        ;;
      CREATE_FAILED|ROLLBACK_COMPLETE|ROLLBACK_FAILED|DELETE_FAILED|UPDATE_ROLLBACK_COMPLETE|UPDATE_ROLLBACK_FAILED|ROLLBACK_IN_PROGRESS)
        echo "Stack creation failed: $stackStatus"
        return 1
        ;;
      *)
        retries=$((retries - 1))
        echo "Waiting for stack creation to complete..."
        sleep 30
        ;;
    esac
  done

  echo "Stack creation did not complete in time"
  return 1
}

do_delete_stack() {
  local _sn=$1
  local rollbackStatus
  rollbackStatus=$(aws cloudformation delete-stack \
    --stack-name $_sn \
    --profile $AWS_PROFILE)

  # wait for stack deletion to complete or fail
  start_time=$(date +%s)
  while true; do
    rollbackStatus=$(aws cloudformation describe-stacks \
      --stack-name $_sn \
      --profile $AWS_PROFILE \
      --query "Stacks[0].StackStatus" \
      --output text)
    if [[ $rollbackStatus == "DELETE_COMPLETE" || $rollbackStatus == "DELETE_FAILED" ]]; then
      echo "Stack Status: $rollbackStatus"
      break
    elif [[ $rollbackStatus == *"ValidationError"* ]]; then
      echo "Stack has been deleted"
      break
    else
      echo "Stack Status: $rollbackStatus"
      elapsed_time=$(($(date +%s) - $start_time))
      echo "Elapsed time: $elapsed_time seconds"
      sleep 30

      # check if we have exceeded the timeout
      if [[ $elapsed_time -ge $DELETE_STACK_TIMEOUT_SECONDS ]]; then
        echo "Timeout exceeded while waiting for stack deletion to complete."
        break
      fi
    fi
  done
}


### hyper variables ###
stackName='SJ-Udacity-Mod2-Redshift'
templateBody='multi-node-cluster-sony.yaml'
echo ">> Stack Name: $stackName"
echo ">> Template Body: $templateBody"

# Tests for stack
#stackStatus=$(does_stack_exist $stackName)
#echo ">> Stack Status: $stackStatus"

### main ###
get_caller_identity
do_create_stack $stackName $templateBody
#do_delete_stack $stackName


echo "\n>>> END <<<"

: <<todo
lint BEFORE creating stack
tag stack
add more features to single-node-stack from udacity_redshift
only delete stack if invalid status or get prompted
todo
