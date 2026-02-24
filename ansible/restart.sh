set -x
set -e

cd /var/tmp/wsklogs
rm -rf *

cd ~/Serverless/downsizing/openwhisk/ansible/

#export ENVIRONMENT=local
export ENVIRONMENT=distributed

ansible-playbook -i environments/$ENVIRONMENT setup.yml -e  docker_image_tag=$OW_TAG_SODA
ansible-playbook -i environments/$ENVIRONMENT couchdb.yml -e  docker_image_tag=$OW_TAG_SODA
ansible-playbook -i environments/$ENVIRONMENT initdb.yml -e  docker_image_tag=$OW_TAG_SODA
ansible-playbook -i environments/$ENVIRONMENT wipe.yml -e  docker_image_tag=$OW_TAG_SODA
#ansible-playbook -i environments/$ENVIRONMENT openwhisk.yml -e  docker_image_tag=$OW_TAG_SODA
ansible-playbook -i environments/$ENVIRONMENT kafka.yml -e  docker_image_tag=$OW_TAG_SODA
ansible-playbook -i environments/$ENVIRONMENT controller.yml -e  docker_image_tag=$OW_TAG_SODA
ansible-playbook -i environments/$ENVIRONMENT invoker.yml -e  docker_image_tag=$OW_TAG_SODA
ansible-playbook -i environments/$ENVIRONMENT edge.yml -e  docker_image_tag=$OW_TAG_SODA
ansible-playbook -i environments/$ENVIRONMENT downloadcli.yml -e  docker_image_tag=$OW_TAG_SODA
ansible-playbook -i environments/$ENVIRONMENT postdeploy.yml -e  docker_image_tag=$OW_TAG_SODA
ansible-playbook -i environments/$ENVIRONMENT apigateway.yml -e  docker_image_tag=$OW_TAG_SODA
ansible-playbook -i environments/$ENVIRONMENT routemgmt.yml -e  docker_image_tag=$OW_TAG_SODA


# deploy functions
cd ~/Serverless/RainbowCake-ASPLOS24/applications/
./deploy_quick.sh

cd ~/benchmarks/serverless/functionBench/
./deploy_functions.sh
