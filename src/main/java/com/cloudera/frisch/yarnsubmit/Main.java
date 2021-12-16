package com.cloudera.frisch.yarnsubmit;


import com.cloudera.frisch.yarnsubmit.config.CommandLineArgumentsParser;
import com.cloudera.frisch.yarnsubmit.config.Parameters;
import com.cloudera.frisch.yarnsubmit.services.HdfsService;
import com.cloudera.frisch.yarnsubmit.services.KerberosService;
import com.cloudera.frisch.yarnsubmit.services.YarnApplicationService;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;


public class Main {

    private static final Logger logger = Logger.getLogger(Main.class);

    public static void main(String[] args) {

        logger.info("Start up Yarn-Submit program");

        Map<CommandLineArgumentsParser.Arguments, String> arguments = CommandLineArgumentsParser.getMainCommandLineArguments(args);

        Parameters.initParameters(arguments.get(CommandLineArgumentsParser.Arguments.CONFIG_FILE));

        logger.info("Initialize Yarn submit service and load configuration ");
        YarnApplicationService yarnApplicationService = new YarnApplicationService();

        if (Boolean.TRUE.equals(Parameters.KERBEROS)) {
            KerberosService.loginUserWithKerberos(Parameters.KERBEROS_USER, Parameters.KEYTAB, yarnApplicationService.getConf());
        }


        HdfsService hdfsService = new HdfsService(yarnApplicationService.getConf());
        logger.info("Clean HDFS directory");
        hdfsService.cleanDirectory(Parameters.HDFS_WORK_DIRECTORY);
        List<String> listOfRequiredLocalFiles = new ArrayList<>();
        listOfRequiredLocalFiles.add(Parameters.KEYTAB);
        listOfRequiredLocalFiles.add(Parameters.JAR_PATH);
        listOfRequiredLocalFiles.add(arguments.get(CommandLineArgumentsParser.Arguments.CONFIG_FILE));
        listOfRequiredLocalFiles.add(Utils.getJarFilePath());
        if (!Parameters.APP_FILES.isEmpty() && !Parameters.APP_FILES.get(0).isEmpty()) {
            listOfRequiredLocalFiles.addAll(Parameters.APP_FILES);
        }
        logger.info("Copy all required files to HDFS directory: " + Parameters.HDFS_WORK_DIRECTORY);
        for (String file : listOfRequiredLocalFiles) {
            hdfsService.copyFileToHDFS(file, Parameters.HDFS_WORK_DIRECTORY + Utils.getFileNameFromPath(file), yarnApplicationService.getConf());
        }
        // Transform list of local files to a list of hdfs files
        List<String> listOfRequiredHdfsFiles = Utils.changePrefixOfFilesPath(listOfRequiredLocalFiles, Parameters.HDFS_WORK_DIRECTORY);

        logger.info("Create YARN client");
        yarnApplicationService.createYarnClient();

        logger.info("Create YARN application");
        yarnApplicationService.createApplication();

        logger.info("Set up container context");
        ContainerLaunchContext containerLaunchContext = yarnApplicationService.createContainerContext(listOfRequiredHdfsFiles,
                Parameters.JAVA_HOME + " -cp yarn-submit.jar com.cloudera.frisch.yarnsubmit.master.JavaMaster ",
                hdfsService.getFileSystem());

        logger.info("Create Submit application context");
        yarnApplicationService.createSubmitContextOfApplication(Parameters.APP_QUEUE, Parameters.APP_PRIORITY,
                Parameters.APP_AM_MEMORY, Parameters.APP_AM_VCORES, Parameters.APP_NAME, containerLaunchContext);

        logger.info("Submit application");
        yarnApplicationService.submitApplication();

        // Get application status and writes it back every X seconds
        printReportWhileApplicationIsNotFinished(yarnApplicationService);


        logger.info("Finish Yarn-Submit program");

    }

    private static void printReportWhileApplicationIsNotFinished(YarnApplicationService yarnApplicationService) {
        boolean isFinished = false;
        while (!isFinished) {
            try {
                ApplicationReport applicationReport = yarnApplicationService.getApplicationReport();
                if (applicationReport != null) {
                    logger.info(yarnApplicationService.getAppProgress(applicationReport));

                    if (Boolean.TRUE.equals(yarnApplicationService.applicatonEnded(applicationReport))) {
                        logger.info("Application is finished with status : " + applicationReport.getYarnApplicationState());
                        logger.info("To get logs of the application, please run: yarn logs -applicationId " + applicationReport.getApplicationId());
                        isFinished = true;
                    }
                }
                Thread.sleep(Parameters.APP_CHECK_STATUS_INTERVAL);
            } catch (InterruptedException e) {
                logger.error("Cannot get app status from Yarn", e);
                Thread.currentThread().interrupt();
                isFinished = true;
            }

        }
    }


}
