set -x
set -e

cd /var/tmp/wsklogs
rm -rf *

cd ~/Serverless/openwhisk/ansible

export ENVIRONMENT=local

ansible-playbook -i environments/$ENVIRONMENT couchdb.yml; 
ansible-playbook -i environments/$ENVIRONMENT initdb.yml; 
ansible-playbook -i environments/$ENVIRONMENT wipe.yml; 
ansible-playbook -i environments/$ENVIRONMENT openwhisk.yml; 
ansible-playbook -i environments/$ENVIRONMENT postdeploy.yml; 
ansible-playbook -i environments/$ENVIRONMENT apigateway.yml; 
ansible-playbook -i environments/$ENVIRONMENT routemgmt.yml

cd ~/benchmarks/serverless/rainbowCake
./deploy_functions.sh
