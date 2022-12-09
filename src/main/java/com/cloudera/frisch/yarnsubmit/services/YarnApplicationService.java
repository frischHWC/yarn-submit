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
package com.cloudera.frisch.yarnsubmit.services;


import lombok.Getter;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;


/**
 * This service contains all required steps and objects to create a yarn application and launch it
 */
public class YarnApplicationService extends YarnService {

    @Getter
    private YarnClient yarnClient;
    @Getter
    private YarnClientApplication yarnClientApplication;
    @Getter
    private ApplicationSubmissionContext applicationSubmissionContext;

    public YarnApplicationService() {
        super();
    }

    public void createYarnClient() {
        this.yarnClient = YarnClient.createYarnClient();
        yarnClient.init(this.conf);
        yarnClient.start();
    }

    public void createApplication() {
        try {
            yarnClientApplication = yarnClient.createApplication();
            GetNewApplicationResponse appResponse = yarnClientApplication.getNewApplicationResponse();
            logger.info("Application created successfully with ApplicationID: " + appResponse.getApplicationId());
        } catch (Exception e) {
            logger.error("An exception occured while trying to create the app", e);
            System.exit(1);
        }
    }

    public void createSubmitContextOfApplication(String queue, int priority, int memory, int vcores, String appName, ContainerLaunchContext ctx) {
        applicationSubmissionContext = yarnClientApplication.getApplicationSubmissionContext();
        applicationSubmissionContext.setApplicationId(yarnClientApplication.getNewApplicationResponse().getApplicationId());
        applicationSubmissionContext.setApplicationName(appName);
        // Add the AM container to the submit context
        applicationSubmissionContext.setAMContainerSpec(ctx);
        applicationSubmissionContext.setQueue(queue);
        applicationSubmissionContext.setPriority(Priority.newInstance(priority));
        applicationSubmissionContext.setResource(Resource.newInstance(memory, vcores));
    }

    public void submitApplication() {
        try {
            yarnClient.submitApplication(applicationSubmissionContext);
        } catch (Exception e) {
            logger.error("Cannot submit application due to failure", e);
            System.exit(2);
        }
    }

    public ApplicationReport getApplicationReport() {
        try {
            return yarnClient.getApplicationReport(yarnClientApplication.getNewApplicationResponse().getApplicationId());
        } catch (Exception e) {
            logger.error("Cannot get app status from Yarn, for application: " +
                    yarnClientApplication.getNewApplicationResponse().getApplicationId() + " due to error: ", e);
        }
        return null;
    }

    public Boolean applicatonEnded(ApplicationReport applicationReport) {
        YarnApplicationState state = applicationReport.getYarnApplicationState();
        return state.equals(YarnApplicationState.FINISHED) ||
                state.equals(YarnApplicationState.FAILED) ||
                state.equals(YarnApplicationState.KILLED);
    }

    public String getAppProgress(ApplicationReport applicationReport) {
        return "Application: " + applicationReport.getApplicationId() +
                " is in state " + applicationReport.getYarnApplicationState().toString() +
                " with a progress status of " + applicationReport.getProgress() * 100 + "%";
    }
}
