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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.io.IOUtils;
import org.apache.log4j.Logger;

import java.io.*;
import java.net.URI;
import java.util.Arrays;
import java.util.List;

/**
 * Provides an HDFS client based on configuration file of the platform
 * And some functions related to HDFS
 */
public class HdfsService {

    private static final Logger logger = Logger.getLogger(HdfsService.class);

    @Getter
    private FileSystem fileSystem;

    public HdfsService(Configuration conf){
        try {
            fileSystem = FileSystem.get(URI.create(conf.get(HdfsClientConfigKeys.DFS_NAMESERVICES)), conf);
        } catch (IOException e) {
            logger.error("Could not connect to HDFS, verify your configuration, due to error: ", e);
        }
    }

    public void copyFileToHDFS(String inpuPath, String outputPath, Configuration conf) {
        try(OutputStream os = fileSystem.create(new Path(outputPath))) {
            InputStream is = new BufferedInputStream(new FileInputStream(inpuPath));
            IOUtils.copyBytes(is, os, conf);
            os.flush();
            logger.info("Copied file: " + inpuPath + " to hdfs output: " + outputPath);
        } catch (IOException e) {
            logger.error("Could not copy file " + inpuPath +" to HDFS : " + outputPath + " due to error: ", e);
        }
    }

    public void cleanDirectory(String path) {
        try {
            fileSystem.delete(new Path(path), true);
            logger.info("Deleted directory and its content: " + path);
        } catch (IOException e) {
            logger.error("Could not delete HDFS path: " + path + " due to error:", e);
        }
    }


    /**
     * Read a file in HDFS and return its content (in one shot)
     * => BE CAREFUL TO THE SIZE OF THE FILE !!!
     * @param path of the file in HDFS
     * @return its content as a full String
     */
    public String readFile(String path) {
        StringBuilder read = new StringBuilder(10000);
        try(FSDataInputStream fsDataInputStream = fileSystem.open(new Path(path))) {
            int length = 1000;
            int numberOfBytesRead;
            byte[] byteArray = new byte[length];
            do {
                numberOfBytesRead = fsDataInputStream.read(byteArray);
                if(numberOfBytesRead != -1) {
                    read.append(new String(byteArray));
                }
            } while (numberOfBytesRead != -1);
        } catch (Exception e) {
            logger.error("Can not read file:" + path + " with error : ", e);
        }
        return read.toString();
    }

    /**
     * Read a file in HDFS and return its content as List of lines (each of it being a String naturally ;) )
     * @param path of the file in HDFS
     * @return its content as a full String
     */
    public List<String> readFileAsList(String path) {
        return Arrays.asList(readFile(path).split("\\n"));
    }

}
