package com.cloudera.frisch.yarnsubmit.services;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;

/**
 * Collection of functions Kerberos related
 */
public class KerberosService {

    private KerberosService() { throw new IllegalStateException("Kerberos Service class not instantiable"); }

    private static final Logger logger = Logger.getLogger(KerberosService.class);

    /**
     * Login to kerberos using a given user and its associated keytab
     * @param kerberosUser is the kerberos user
     * @param pathToKeytab path to the keytab associated with the user, note that unix read-right are needed to access it
     * @param config hadoop configuration used further
     */
    public static void loginUserWithKerberos(String kerberosUser, String pathToKeytab, Configuration config) {
        if(config != null) {
            config.set("hadoop.security.authentication", "kerberos");
            UserGroupInformation.setConfiguration(config);
        }
        try {
            UserGroupInformation.loginUserFromKeytab(kerberosUser, pathToKeytab);
        } catch (IOException e) {
            logger.error("Could not load keytab file",e);
        }
    }

    /**
     * get the Kerberos tokens for the logged user (so implicitly the YARN_AM_RM)
     * and get an HDFS token before adding it
     * @param conf Hadoop configuration
     * @return all the tokens in a form of a byteBuffer (required by YARN API)
     */
    public static ByteBuffer getTokensFromUserLogged(Configuration conf) {
        ByteBuffer byteBuffer = null;
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dataOutputStream = new DataOutputStream(baos);
        try {
            Credentials credentials = UserGroupInformation.getLoginUser().getCredentials();
            FileSystem fileSystem = FileSystem.get(URI.create(conf.get(HdfsClientConfigKeys.DFS_NAMESERVICES)), conf);
            fileSystem.addDelegationTokens("", credentials);

            credentials.writeTokenStorageToStream(dataOutputStream);
            dataOutputStream.flush();
            byteBuffer = ByteBuffer.wrap(baos.toByteArray());
        } catch (IOException e) {
            logger.info("Can not get any tokens due to error, ", e);
        }
        return byteBuffer;
    }

    /**
     * List all kerberos tokens available for logged in user
     */
    public static void listTokensAvailable() {
        try {
            UserGroupInformation.getLoginUser().getCredentials().getAllTokens().forEach(token -> logger.info("Token: " + token.toString()));
        } catch (Exception e) {
            logger.error("Cant list tokens due to", e);
        }
    }

}
