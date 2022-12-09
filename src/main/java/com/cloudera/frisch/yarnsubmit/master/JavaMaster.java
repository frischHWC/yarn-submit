/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cloudera.frisch.yarnsubmit.master;

import com.cloudera.frisch.yarnsubmit.Utils;
import com.cloudera.frisch.yarnsubmit.config.Parameters;
import com.cloudera.frisch.yarnsubmit.object.YarnContainer;
import com.cloudera.frisch.yarnsubmit.services.HdfsService;
import com.cloudera.frisch.yarnsubmit.services.KerberosService;
import com.cloudera.frisch.yarnsubmit.services.YarnAmService;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class JavaMaster {

    private static final Logger logger = Logger.getLogger(JavaMaster.class);

    public static void main(String[] args) {

        logger.info("Starting AM");

        // List all tokens we have in DEBUG mode
        if (logger.isDebugEnabled()) {
            KerberosService.listTokensAvailable();
        }

        logger.info("Load Parameters from parameters.properties file automatically setup in classpath");
        Parameters.initParameters("parameters.properties");

        // Even if all required files are passed in classpath, it is necessary to locate them in HDFS and pass them to future containers
        List<String> listOfRequiredFiles = new ArrayList<>();
        listOfRequiredFiles.add(Parameters.KEYTAB);
        listOfRequiredFiles.add(Parameters.JAR_PATH);
        listOfRequiredFiles.addAll(Parameters.APP_FILES);
        List<String> listOfRequiredHdfsFiles = Utils.changePrefixOfFilesPath(listOfRequiredFiles, Parameters.HDFS_WORK_DIRECTORY);


        logger.info("Initialize Yarn AM service");
        YarnAmService yarnAmService = new YarnAmService();

        logger.info("Initialize HDFS service");
        HdfsService hdfsService = new HdfsService(yarnAmService.getConf());

        logger.info("Initialize ResourceManager and NodeManager clients");
        yarnAmService.createAmAndRmclient();

        logger.info("Register Application Master to Yarn");
        yarnAmService.registerAmToYarn();

        logger.info("Retrieving commands to launch and set them up");
        List<YarnContainer> containers = new ArrayList<>();
        JavaMaster.commandListToLaunch(hdfsService).forEach(command -> containers.add(new YarnContainer(command)));

        logger.info("Asking to launch " + containers.size() + " containers");
        for (int i = 0; i < containers.size(); i++) {
            yarnAmService.getRmClient().addContainerRequest(
                    yarnAmService.setupContainerAskForRM(Parameters.APP_CONTAINER_MEMORY, Parameters.APP_CONTAINER_VCORES, Parameters.APP_PRIORITY)
            );
        }

        // Loop while not all containers to be allocated have been retrieved and have completed their tasks
        int numbersOfContainersToComplete = containers.size();
        int completedContainers = 0;
        while (completedContainers < numbersOfContainersToComplete) {

            // Progression is calculated by checking how many containers have finished compare to the total number of containers to launch (A default 10 is set)
            float progression = completedContainers == 0 && numbersOfContainersToComplete < 10 ?
                    0.1f : (float) completedContainers / (float) numbersOfContainersToComplete;

            try {
                // Retrieve containers by asking RM
                AllocateResponse response = yarnAmService.getRmClient().allocate(progression);

                // Let's launch command in containers by talking to NM directly
                launchcontainers(yarnAmService, containers, response.getAllocatedContainers(), hdfsService, listOfRequiredHdfsFiles);

                // Check results of completed containers
                checkCompletedContainersStatus(yarnAmService, containers, response.getCompletedContainersStatuses());
            } catch (YarnException | IOException e) {
                logger.error("Could not get YARN RM response due to error: ", e);
            }

            logger.info("Waiting for containers to complete");
            try {
                Thread.sleep(Parameters.APP_CHECK_CONTAINERS_COMPLETED);
            } catch (InterruptedException e) {
                logger.error("Error while waiting for containers to complete", e);
                Thread.currentThread().interrupt();
            }

            // Update number of completed containers
            completedContainers = YarnContainer.getNumberOfCompletedContainers(containers);

        }

        logger.info("Unregister Application Master to Resource Manager");
        unregisterMaster(yarnAmService, containers);

        logger.info("Finished AM");

    }


    private static void unregisterMaster(YarnAmService yarnAmService, List<YarnContainer> containers) {
        try {
            if (!YarnContainer.checkAllContainersAreSuccessful(containers)) {
                logger.warn("Some containers did not finished successfully, check with yarn logs for this application");
                yarnAmService.getRmClient().unregisterApplicationMaster(FinalApplicationStatus.FAILED,
                        "Failed due to at least one container failing", null);
            }
            yarnAmService.getRmClient().unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, "Finished", null);
        } catch (YarnException | IOException e) {
            logger.error("Could not unregister master due to error: ", e);
        }
    }

    /**
     * Launch as much as possible containers requested using containers allocated by NM
     *
     * @param yarnAmService
     * @param containers
     * @param containersAllocated
     * @param hdfsService
     * @param listOfRequiredHdfsFiles
     */
    private static void launchcontainers(YarnAmService yarnAmService, List<YarnContainer> containers, List<Container> containersAllocated,
                                         HdfsService hdfsService, List<String> listOfRequiredHdfsFiles) {
        for (Container container : containersAllocated) {
            YarnContainer containerWheretoRun = YarnContainer.findOneAvailableCommandToLaunch(containers);

            if (containerWheretoRun == null) {
                logger.warn("Not able to find a needed command to launch, so cancelling container: " + container.getId().getContainerId());
                yarnAmService.getRmClient().releaseAssignedContainer(container.getId());
                break;
            }

            ContainerLaunchContext ctx = yarnAmService.createContainerContext(listOfRequiredHdfsFiles,
                    containerWheretoRun.getCommand(), hdfsService.getFileSystem());
            containerWheretoRun.setContainerRunning(container.getId().getContainerId());

            logger.info("Launch a container: " + containerWheretoRun.toString());
            try {
                yarnAmService.getNmClient().startContainer(container, ctx);
            } catch (YarnException | IOException e) {
                logger.warn("Could not allocate a container due to error: ", e);
            }
        }
    }

    /**
     * Check status of completed containers and if it is successful,
     * If not, It will retry to launch it 3 times before considering it as finished with a failed status
     *
     * @param yarnAmService
     * @param containers
     * @param containersCompletedStatus
     */
    private static void checkCompletedContainersStatus(YarnAmService yarnAmService, List<YarnContainer> containers, List<ContainerStatus> containersCompletedStatus) {
        for (ContainerStatus container : containersCompletedStatus) {
            logger.info("Container : " + container.getContainerId() + " has finished its tasks");
            YarnContainer containerFinished = YarnContainer.findContainerUsingContainerId(containers, container.getContainerId().getContainerId());
            if (containerFinished == null) {
                logger.error("Unable to retrieve ended container, please check YARN NM logs");
                continue;
            }
            containerFinished.setFinalState(YarnContainer.state.FINISHED);

            // Check if container exited with a non-successful return code
            if (container.getExitStatus() != 0) {
                logger.warn("Container : " + container.getContainerId() + " failed with exit code " + container.getExitStatus() +
                        "due to error: " + container.getState().name());

                if (containerFinished.getTries() < 3) {
                    logger.info("Container will be retried as there are less than 3 retries for this container: " + containerFinished.toString());
                    yarnAmService.getRmClient().addContainerRequest(
                            yarnAmService.setupContainerAskForRM(Parameters.APP_CONTAINER_MEMORY, Parameters.APP_CONTAINER_VCORES, Parameters.APP_PRIORITY)
                    );
                    containerFinished.resetContainerToRun();

                } else {
                    logger.warn("Container has been retried more than 3, it should not be retried: " + container.toString());
                    containerFinished.setSuccessful(false);
                }
            } else {
                containerFinished.setSuccessful(true);
            }
        }
    }

    /**
     * List all commands to launch on containers, either by duplicating one to the number of necessary times or
     * reading file listing all commands
     *
     * @param hdfsService
     * @return
     */
    private static List<String> commandListToLaunch(HdfsService hdfsService) {
        List<String> commandsToLaunch = new ArrayList<>();

        if (Parameters.JAR_PATH.endsWith(".jar")) {
            logger.info("This is a jar file, there will be " + Parameters.APP_CONTAINER_NUMBER + " containers launched with that jar");
            for (int i = 0; i < Parameters.APP_CONTAINER_NUMBER; i++) {
                commandsToLaunch.add("java -jar " + Utils.getFileNameFromPath(Parameters.JAR_PATH) + " " + Parameters.JAR_ARGUMENTS);
            }
        } else {
            logger.info("File: " + Parameters.HDFS_WORK_DIRECTORY + Utils.getFileNameFromPath(Parameters.JAR_PATH) + " will be read from HDFS to determine which commands to launch");
            commandsToLaunch.addAll(hdfsService.readFileAsList(Parameters.HDFS_WORK_DIRECTORY + Utils.getFileNameFromPath(Parameters.JAR_PATH)));
        }

        return commandsToLaunch;
    }

}
