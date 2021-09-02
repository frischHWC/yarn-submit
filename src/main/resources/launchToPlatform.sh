#!/usr/bin/env bash

# Export Machine name to be  able to launch this script
# export HOST=

export USER=root
export DIR=/root/yarn-submit

# Send yarn-submit.jar & Config file & yarn-submit.sh
ssh ${USER}@${HOST} "rm -rf ${DIR}/"
ssh ${USER}@${HOST} "mkdir -p ${DIR}/"
scp target/yarn-submit-*.jar ${USER}@${HOST}:${DIR}/yarn-submit.jar
scp src/main/resources/parameters.properties ${USER}@${HOST}:${DIR}/
scp src/main/resources/yarn-submit.sh ${USER}@${HOST}:${DIR}/
scp src/main/resources/commandsToLaunch ${USER}@${HOST}:${DIR}/
ssh ${USER}@${HOST} "chmod +x ${DIR}/yarn-submit.sh"

# SSH on the machine and run or run it directly from here:
ssh ${USER}@${HOST} "cd ${DIR} ;
./yarn-submit.sh \
    --app-name=random-datagen \
    --container-memory=4096 \
    --kerberos-user=dev@FRISCH.COM \
    --keytab=/home/dev/dev.keytab \
    --app-files=/root/random-datagen/config.properties,/root/random-datagen/log4j2.properties,/root/random-datagen/log4j.properties,/root/random-datagen/employee.json,/root/random-datagen/random-datagen.jar \
    /root/yarn-submit/commandsToLaunch"

#
#./yarn-submit.sh \
#    --app-name=kafka-stream-test \
#    --container-memory=2048 \
#    --container-number=3 \
#    --kerberos-user=dev@FRISCH.COM \
#    --keytab=/home/dev/dev.keytab \
#    --app-files=/home/root/kafka-streams-test/config.properties,/home/root/kafka-streams-test/log4j.properties \
#    /home/root/kafka-streams-test/kafka-streams-tester.jar"
#