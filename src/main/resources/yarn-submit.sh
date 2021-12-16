#!/usr/bin/env bash

APP_FILES=""
APP_NAME="submit-test"
HDFS_WORK_DIR=""
KERBEROS_KEYTAB=""
KERBEROS_USER=""
APP_QUEUE="default"
APP_PRIORITY="0"
APP_AM_MEMORY="1024"
APP_AM_VCORES="1"
APP_CONTAINER_MEMORY="1024"
APP_CONTAINER_VCORES="1"
APP_CONTAINER_NUMBER="1"
YARN_SITE="/etc/hadoop/conf.cloudera.yarn/yarn-site.xml"
HDFS_SITE="/etc/hadoop/conf.cloudera.hdfs/hdfs-site.xml"
CORE_SITE="/etc/hadoop/conf.cloudera.yarn/core-site.xml"
HADOOP_USERNAME="$USER"
HADOOP_HOME="/user/$USER"
CHECK_STATUS_INTERVAL="1000"
JAVA_HOME="/usr/bin/java"


function usage()
{
    echo "This is the Yarn Submit tools, it submits any kind of Java application to YARN and creates as many instances of this application as required"
    echo ""
    echo "Under the hood, it will instantiate an Application Master, handle all dialogs with YARN and HDFS, and creates YARN containers where the application will be launched  "
    echo ""
    echo "Usage is the following : "
    echo ""
    echo "./yarn-submit.sh"
    echo "  -h --help"
    echo "  --app-files=$APP_FILES : (Optional) List of files separated by a ',' required by the java application (Default) "
    echo "  --app-name=$APP_NAME : (Optional) App name in Yarn (Default) submit-test"
    echo "  --hdfs-work-dir=$HDFS_WORK_DIR : (Optional) HDFS path where to work (Default) /tmp/yarnsubmit/ + application Name + Time in ms"
    echo "  --keytab=$KERBEROS_KEYTAB : (Optional) path to keytab to use if kerberos is used (Default) "
    echo "  --kerberos-user=$KERBEROS_USER : (Optional) principal@REALM.COM associated to the keytab (Default) "
    echo "  --queue=$APP_QUEUE : (Optional) queue where to launch the application (and the master) (Default) default"
    echo "  --priority=$APP_PRIORITY : (Optional) priority of the application for Yarn, as a number >= 0 (Default) 0"
    echo "  --am-memory=$APP_AM_MEMORY : (Optional) memory allocated for the master in MB (Default) 1024"
    echo "  --am-vcores=$APP_AM_VCORES : (Optional) vcores allocated for the master (Default) 1"
    echo "  --container-memory=$APP_CONTAINER_MEMORY : (Optional) memory allocated for each container in MB (Default) 1024"
    echo "  --container-vcores=$APP_CONTAINER_VCORES : (Optional) vcores allocated for each container (Default) 1"
    echo "  --container-number=$APP_CONTAINER_NUMBER : (Optional) Number of containers to launch (Default) 1"
    echo "  --yarn-site=$YARN_SITE : (Optional) path to the yarn-site on each machine of the cluster (Default) /etc/hadoop/conf.cloudera.yarn/yarn-site.xml"
    echo "  --hdfs-site=$HDFS_SITE : (Optional) path to the hdfs-site on each machine of the cluster (Default) /etc/hadoop/conf.cloudera.hdfs/hdfs-site.xml"
    echo "  --core-site=$CORE_SITE : (Optional) path to the core-site on each machine of the cluster (Default) /etc/hadoop/conf.cloudera.yarn/core-site.xml"
    echo "  --hadoop-username=$HADOOP_USERNAME : (Optional) hadoop username (Default) "
    echo "  --hadoop-home=$HADOOP_HOME : (Optional) hadoop home of the user (Default) "
    echo "  --java-home=$JAVA_HOME : (Optional) Path to Java, to use a non default one (Default) /usr/bin/java "
    echo "  --check-status-interval=$CHECK_STATUS_INTERVAL: (Optional) Time in ms between each check of application status from the client to the AM (Default) 1000"
    echo "<JarPath> <Args> OR <filePath>"
    echo ""
}

echo "Launching application using yarn-submit.sh"

# Get Jar and jar args from command line
JAR_AND_JAR_ARGS=$(echo $@ | sed 's/--[^ ]*//g')
if [[ -z ${JAR_AND_JAR_ARGS} ]]
then
    echo "yarn-submit.sh requires at least a jar or a file (filled with different lines, each of it will be launched on a container) in argument"
    exit
fi
JAR="$(echo ${JAR_AND_JAR_ARGS} | awk '{print $1;}')"
ARGS="$(echo ${JAR_AND_JAR_ARGS} | awk '{$1 = ""; print $0; }')"

# Create parameters empty file
PARAMETERS_FILE=parameters.properties
rm -rf ${PARAMETERS_FILE}
touch ${PARAMETERS_FILE}

# Populate file with jar and its args
echo "jar.path=$JAR" >> ${PARAMETERS_FILE}
echo "jar.arguments=$ARGS" >> ${PARAMETERS_FILE}

# Populate parameters properties file with all parameters passed to the command line
while [ "$1" != "" ]; do
    PARAM=`echo $1 | awk -F= '{print $1}'`
    VALUE=`echo $1 | awk -F= '{print $2}'`
    case $PARAM in
        -h | --help)
            usage
            exit
            ;;
        --app-files)
            APP_FILES=$VALUE
            echo "app.files=$VALUE" >> ${PARAMETERS_FILE}
            ;;
        --app-name)
            APP_NAME=$VALUE
            echo "app.name=$VALUE" >> ${PARAMETERS_FILE}
            ;;
        --hdfs-work-dir)
            HDFS_WORK_DIR=$VALUE
            echo "hdfs.work.directory=$VALUE" >> ${PARAMETERS_FILE}
            ;;
        --keytab)
            KERBEROS_KEYTAB=$VALUE
            echo "kerberos.keytab=$VALUE" >> ${PARAMETERS_FILE}
            ;;
        --kerberos-user)
            KERBEROS_USER=$VALUE
            echo "kerberos=true" >> ${PARAMETERS_FILE}
            echo "kerberos.user=$VALUE" >> ${PARAMETERS_FILE}
            ;;
        --queue)
            APP_QUEUE=$VALUE
            echo "app.queue=$VALUE" >> ${PARAMETERS_FILE}
            ;;
        --priority)
            APP_PRIORITY=$VALUE
            echo "app.priority=$VALUE" >> ${PARAMETERS_FILE}
            ;;
         --am-memory)
            APP_AM_MEMORY=$VALUE
            echo "app.am.memory=$VALUE" >> ${PARAMETERS_FILE}
            ;;
        --am-vcores)
            APP_AM_VCORES=$VALUE
            echo "app.am.vcores=$VALUE" >> ${PARAMETERS_FILE}
            ;;
        --container-memory)
            APP_CONTAINER_MEMORY=$VALUE
            echo "app.container.memory=$VALUE" >> ${PARAMETERS_FILE}
            ;;
        --container-vcores)
            APP_CONTAINER_VCORES=$VALUE
            echo "app.container.vcores=$VALUE" >> ${PARAMETERS_FILE}
            ;;
        --container-number)
            APP_CONTAINER_NUMBER=$VALUE
            echo "app.container.number=$VALUE" >> ${PARAMETERS_FILE}
            ;;
        --yarn-site)
            YARN_SITE=$VALUE
            echo "yarn.site=$VALUE" >> ${PARAMETERS_FILE}
            ;;
        --hdfs-site)
            HDFS_SITE=$VALUE
            echo "hdfs.site=$VALUE" >> ${PARAMETERS_FILE}
            ;;
        --core-site)
            CORE_SITE=$VALUE
            echo "core.site=$VALUE" >> ${PARAMETERS_FILE}
            ;;
        --hadoop-username)
            HADOOP_USERNAME=$VALUE
            echo "hadoop.username=$VALUE" >> ${PARAMETERS_FILE}
            ;;
        --hadoop-home)
            HADOOP_HOME=$VALUE
            echo "hadoop.home=$VALUE" >> ${PARAMETERS_FILE}
            ;;
        --check-status-interval)
            CHECK_STATUS_INTERVAL=$VALUE
            echo "app.check-status-interval=$VALUE" >> ${PARAMETERS_FILE}
            ;;
        --java-home)
            JAVA_HOME=$VALUE
            echo "java.home=$VALUE" >> ${PARAMETERS_FILE}
            ;;
        *)
            ;;
    esac
    shift
done

# If HDFS work directory is not precise, it should be set to a random one
if [ -z $HDFS_WORK_DIR ]
then
    echo "hdfs.work.directory=/tmp/yarnsubmit/${APP_NAME}/$(date +%s)/" >> ${PARAMETERS_FILE}
fi


echo "Constructed following $PARAMETERS_FILE with content:"
cat $PARAMETERS_FILE

echo "Launch of java command for yarn-submit.jar"

$JAVA_HOME --add-opens java.base/jdk.internal.ref=ALL-UNNAMED -jar yarn-submit.jar -c $PARAMETERS_FILE

echo "Finished application using yarn-submit.sh"



