package com.cloudera.frisch.yarnsubmit.services;

import lombok.Getter;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.NMClient;

public class YarnAmService extends YarnService {

    @Getter
    AMRMClient<AMRMClient.ContainerRequest> rmClient;
    @Getter
    NMClient nmClient;


    public YarnAmService() {
        super();
    }

    // TODO: Be able to handle renewal of tokens in AM (see: https://hadoop.apache.org/docs/current/hadoop-yarn/hadoop-yarn-site/YarnApplicationSecurity.html )

    // TODO: Make use of Async Client & use threads to handle them in the background (see: https://hadoop.apache.org/docs/current/hadoop-yarn/hadoop-yarn-site/WritingYarnApplications.html )

    public void createAmAndRmclient() {
        // Instantiate Resource Manager client that will be used to negotiate containers
        /*
        NOTE: It is possible to use more properly the API by creating a CallBackHandler and pass it to the client
          AMRMClientAsync.CallbackHandler allocListener = new RMCallbackHandler();
          rmClient = AMRMClientAsync.createAMRMClientAsync(1000, allocListener);
        Hence, this needs to create a AbstractCallbackHandler by implementing it;
         */
        this.rmClient = AMRMClient.createAMRMClient();
        rmClient.init(conf);
        rmClient.start();

        // Instantiate Node Manager client that will be used to instantiate containers
         /*
        NOTE: It is possible to use more properly the API by creating a CallBackHandler and pass it to the client
          NMClientAsync.createNMClientAsync() should be used and an AbstractCallbackHandler should be passed
        Hence, this needs to create a AbstractCallbackHandler by implementing it;
         */
        this.nmClient = NMClient.createNMClient();
        nmClient.init(conf);
        nmClient.start();
    }

    // TODO: Check to set better parameters than default one
    public void registerAmToYarn() {
        try {
            rmClient.registerApplicationMaster(NetUtils.getHostname(), 12345, "");
        } catch (Exception e) {
            logger.error("Cannot Register Application Master: ", e);
            System.exit(1);
        }
    }

    public AMRMClient.ContainerRequest setupContainerAskForRM(int memory, int vCores, int priority) {
        AMRMClient.ContainerRequest request = new AMRMClient.ContainerRequest(Resource.newInstance(memory, vCores),
                null, null, Priority.newInstance(priority));
        logger.info("Requested container ask: " + request.toString());
        return request;
    }


}
