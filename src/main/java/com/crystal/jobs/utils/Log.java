package com.crystal.jobs.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Log {
    public static final Logger logger = LoggerFactory.getLogger(Log.class);
    public static void logInfo(String msg){
        logger.info(msg);
    }
}
