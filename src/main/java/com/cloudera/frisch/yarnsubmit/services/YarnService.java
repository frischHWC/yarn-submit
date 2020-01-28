package com.cloudera.frisch.yarnsubmit.services;


import com.cloudera.frisch.yarnsubmit.Utils;
import com.cloudera.frisch.yarnsubmit.config.Parameters;
import lombok.Getter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Provides a YARN global service (that needs to be extended by YARN master and application services)
 */
public class YarnService {

    static final Logger logger = Logger.getLogger(YarnService.class);

    @Getter
    YarnConfiguration conf;

    /**
     * Instantiate a Yarn Configuration fully setup
     *
     * @return
     */
    YarnService() {
        this.conf = new YarnConfiguration();
        this.conf.addResource(new Path(Parameters.CORE_SITE));
        this.conf.addResource(new Path(Parameters.HDFS_SITE));
        this.conf.addResource(new Path(Parameters.YARN_SITE));
        System.setProperty("HADOOP_USER_NAME", Parameters.HADOOP_USER_NAME);
        System.setProperty("hadoop.home.dir", Parameters.HADOOP_HOME_DIR);
    }

    /**
     * Create a container Context ready to be launched, instantiated with kerberos, hdfs files required, and java command set
     *
     * @param requiredFilesFromHdfs
     * @param command
     * @return
     */
    public ContainerLaunchContext createContainerContext(List<String> requiredFilesFromHdfs, String command, FileSystem fileSystem) {
        // Setup commands to launch one container
        List<String> commands = new ArrayList<>();
        commands.add(command
                + " 1> " + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout "
                + " 2> " + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr ");

        // Setup the container with all required resources (HDFS files, environment var, kerberos tokens)
        return ContainerLaunchContext.newInstance(
                this.getResourcesForContainer(requiredFilesFromHdfs, fileSystem), this.getEnvironment(conf), commands,
                null, KerberosService.getTokensFromUserLogged(conf), null);
    }


    private Map<String, LocalResource> getResourcesForContainer(List<String> requiredFilesFromHdfs, FileSystem fileSystem) {
        Map<String, LocalResource> localResources = new HashMap<>();
        try {
            for (String file : requiredFilesFromHdfs) {
                FileStatus fileStatus = fileSystem.getFileStatus(new Path(file));
                localResources.put(Utils.getFileNameFromPath(file),
                        LocalResource.newInstance(URL.fromPath(new Path(file)),
                                LocalResourceType.FILE, LocalResourceVisibility.PUBLIC,
                                fileStatus.getLen(), fileStatus.getModificationTime()));
                logger.info("File added to container: " + file);
            }

        } catch (IOException e) {
            logger.error("Could not acces HDFS due to: ", e);
        }
        return localResources;
    }

    private Map<String, String> getEnvironment(Configuration conf) {
        Map<String, String> environment = new HashMap<>();
        Apps.addToEnvironment(environment, ApplicationConstants.Environment.CLASSPATH.name(), conf.get(YarnConfiguration.YARN_APPLICATION_CLASSPATH), ":");
        for (String classpath : YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH) {
            Apps.addToEnvironment(environment, ApplicationConstants.Environment.CLASSPATH.name(), conf.get(classpath), ":");
        }
        Apps.addToEnvironment(environment, ApplicationConstants.Environment.CLASSPATH.name(), ApplicationConstants.Environment.PWD.$() + File.separator + "*", ":");

        return environment;
    }


}
