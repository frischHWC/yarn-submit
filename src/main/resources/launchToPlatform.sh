#!/usr/bin/env bash

# Export Machine name to be  able to launch this script
# export MACHINE=

export USER=root
export DIR=/home/root/yarn-submit

# Send yarn-submit.jar & Config file & yarn-submit.sh
ssh ${USER}@${MACHINE} "rm -rf ${DIR}/"
ssh ${USER}@${MACHINE} "mkdir -p ${DIR}/"
scp target/yarn-submit-*.jar ${USER}@${MACHINE}:${DIR}/yarn-submit.jar
scp src/main/resources/parameters.properties ${USER}@${MACHINE}:${DIR}/
scp src/main/resources/yarn-submit.sh ${USER}@${MACHINE}:${DIR}/
scp src/main/resources/commandsToLaunch ${USER}@${MACHINE}:${DIR}/
ssh ${USER}@${MACHINE} "chmod +x ${DIR}/yarn-submit.sh"

# SSH on the machine and run or run it directly from here:
ssh ${USER}@${MACHINE} "cd ${DIR} ;
./yarn-submit.sh \
    --app-name=random-datagen \
    --container-memory=4096 \
    --kerberos-user=dev@DEV.FRISCH.COM \
    --keytab=/home/dev/dev.keytab \
    --app-files=/home/root/random-datagen/config.properties,/home/root/random-datagen/log4j2.properties,/home/root/random-datagen/model.json,/home/root/random-datagen/random-datagen.jar \
    /home/root/yarn-submit/commandsToLaunch"

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