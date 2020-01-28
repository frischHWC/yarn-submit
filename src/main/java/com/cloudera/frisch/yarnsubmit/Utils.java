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
