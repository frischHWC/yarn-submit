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
package com.cloudera.frisch.yarnsubmit.config;

import lombok.Getter;
import org.apache.log4j.Logger;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;


/**
 * Goal of this class is to load all needed Parameters from parameters.properties file
 */
@Getter
public class Parameters {

    private Parameters() { throw new IllegalStateException("Parameters class not instantiable"); }

    private static final Logger logger = Logger.getLogger(Parameters.class);

    public static void initParameters(String pathToConfigPropertiesFile) {
        Properties properties = loadProperties(pathToConfigPropertiesFile);

        YARN_SITE = "file://"+  getProperty(properties,"yarn.site", "/etc/hadoop/conf.cloudera.yarn/yarn-site.xml");
        HDFS_SITE = "file://"+  getProperty(properties,"hdfs.site", "/etc/hadoop/conf.cloudera.yarn/core-site.xml");
        CORE_SITE = "file://"+  getProperty(properties,"core.site",  "/etc/hadoop/conf.cloudera.hdfs/hdfs-site.xml");
        HADOOP_USER_NAME = getProperty(properties,"hadoop.username", "");
        HADOOP_HOME_DIR = getProperty(properties,"hadoop.home", "");
        KERBEROS = Boolean.valueOf(getProperty(properties,"kerberos", "false"));
        KEYTAB = getProperty(properties,"kerberos.keytab", "");
        KERBEROS_USER = getProperty(properties,"kerberos.user", "");
        APP_NAME = getProperty(properties,"app.name", "submit-test");
        APP_QUEUE = getProperty(properties,"app.queue", "default");
        APP_PRIORITY = Integer.valueOf(getProperty(properties,"app.priority", "0"));
        APP_AM_MEMORY = Integer.valueOf(getProperty(properties,"app.am.memory", "1024"));
        APP_AM_VCORES = Integer.valueOf(getProperty(properties,"app.am.vcores", "1"));
        APP_CONTAINER_MEMORY = Integer.valueOf(getProperty(properties,"app.container.memory", "1024"));
        APP_CONTAINER_VCORES = Integer.valueOf(getProperty(properties,"app.container.vcores", "1"));
        APP_CONTAINER_NUMBER = Integer.valueOf(getProperty(properties,"app.container.number", "1"));
        APP_FILES = Arrays.asList(getProperty(properties,"app.files", "").split(","));
        APP_CHECK_STATUS_INTERVAL = Long.valueOf(getProperty(properties,"app.check-status-interval", "1000"));
        APP_CHECK_CONTAINERS_COMPLETED = Long.valueOf(getProperty(properties,"app.check.containers.completed", "1000"));

        HDFS_WORK_DIRECTORY = getProperty(properties,"hdfs.work.directory", "/tmp/yarnsubmit/" + APP_NAME + "/");

        JAVA_HOME = getProperty(properties,"java.home", "/usr/bin/java");
        JAR_PATH = getProperty(properties,"jar.path", "");
        JAR_ARGUMENTS = getProperty(properties,"jar.arguments", "");

    }

    public static String YARN_SITE;
    public static String HDFS_SITE;
    public static String CORE_SITE;
    public static String HADOOP_USER_NAME;
    public static String HADOOP_HOME_DIR;
    public static String HDFS_WORK_DIRECTORY;

    public static Boolean KERBEROS;
    public static String KEYTAB;
    public static String KERBEROS_USER;

    public static String APP_NAME;
    public static String APP_QUEUE;
    public static Integer APP_PRIORITY;
    public static Integer APP_AM_MEMORY;
    public static Integer APP_AM_VCORES;
    public static Integer APP_CONTAINER_MEMORY;
    public static Integer APP_CONTAINER_VCORES;
    public static Integer APP_CONTAINER_NUMBER;
    public static List<String> APP_FILES;
    public static Long APP_CHECK_STATUS_INTERVAL;
    public static Long APP_CHECK_CONTAINERS_COMPLETED ;

    public static String JAVA_HOME;
    public static String JAR_PATH;
    public static String JAR_ARGUMENTS;


    /**
     * Load properties from parameters.properties file
     * @param pathToConfigPropertiesFile
     * @return
     */
    private static Properties loadProperties(String pathToConfigPropertiesFile) {
        java.util.Properties properties = new java.util.Properties();
        try {
            FileInputStream fileInputStream = new FileInputStream(pathToConfigPropertiesFile);
            properties.load(fileInputStream);
        } catch (IOException e) {
            logger.error("Property file not found !", e);
        }
        return properties;
    }

    /**
     * Retrieve a property if it exists (with possibility of having variable references in config file)
     * and return either it or a default value
     * @param properties
     * @param key
     * @return
     */
    @SuppressWarnings("unchecked")
    private static String getProperty(Properties properties, String key, String defaultValue) {
        String property = "";
        try {
            property = properties.getProperty(key);
            if(property==null || property.isEmpty()) {
                property = defaultValue;
            } else {
                if (property.length() > 1 && property.substring(0, 2).equalsIgnoreCase("${")) {
                    property = getProperty(properties, property.substring(2, property.length() - 1), defaultValue);
                }
            }
        } catch (Exception e) {
            logger.warn("Could not get property : " + key + " due to following error: ", e);
        }
        logger.debug("Properties loaded is : " + key + " with value: " + property);
        return property;
    }

}
