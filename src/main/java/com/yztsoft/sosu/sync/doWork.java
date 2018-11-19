package com.yztsoft.sosu.sync;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

/**
 * @classname: doWork
 * @description:
 * @author: Shi Shijie
 * @create: 2018-11-19 17:03
 **/
@Configuration
@EnableScheduling
public class doWork {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Scheduled(cron = "0/10 * * * * ?")
    public void scheduler() {
        logger.info("定时任务执行中......");
    }
}
