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
package com.cloudera.frisch.yarnsubmit;

import org.apache.log4j.Logger;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.List;
import java.util.stream.Collectors;


public class Utils {

    private Utils() { throw new IllegalStateException("Utils class not instantiable"); }

    private static final Logger logger = Logger.getLogger(Utils.class);

    public static String getFileNameFromPath(String filepath) {
        String[] filePathArray = filepath.split("/");
        return filePathArray[filePathArray.length-1];
    }

    public static List<String> changePrefixOfFilesPath(List<String> filesToChangePrefix, String newPrefix) {
        return filesToChangePrefix.stream().map(f -> newPrefix + Utils.getFileNameFromPath(f)).collect(Collectors.toList());
    }

    public static String getJarFilePath() {
        try {
            return URLDecoder.decode(Main.class.getProtectionDomain().getCodeSource().getLocation().getPath(), "UTF-8");
        } catch (UnsupportedEncodingException e) {
            logger.warn("could not access jar file path, hence will use default one of yarn-submit.jar. Error is: ", e);
        }
        return "yarn-submit.jar";
    }

}
