package com.ebay.sojourner.common.util;

import java.util.TimerTask;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.jetbrains.annotations.NotNull;

@Slf4j
public class LkpRefreshTimeTask extends TimerTask {

    private volatile LkpManager lkpManager;

    public LkpRefreshTimeTask(LkpManager lkpManager, TimeUnit timeUnit) {
        this.lkpManager = lkpManager;
        //Set start at 00:05:00 every day
        //        Calendar calendar = Calendar.getInstance();
        //        calendar.set(Calendar.HOUR_OF_DAY, 0);
        //        calendar.set(Calendar.MINUTE, 5);
        //        calendar.set(Calendar.SECOND, 0);
        //        Timer timer = new Timer(true);
        //        timer.scheduleAtFixedRate(this, calendar.getTime(), timeUnit.toMillis(1));
        ScheduledThreadPoolExecutor scheduledThreadPoolExecutor
                = new ScheduledThreadPoolExecutor(1, new ThreadFactory() {
            @Override
            public Thread newThread(@NotNull Runnable r) {
                Thread t = new Thread(r, "sojourner-lkp-refresh-thread");
                t.setDaemon(true);
                return t;
            }
        });
        scheduledThreadPoolExecutor
            .scheduleAtFixedRate(this, 0, 1, timeUnit);
    }

    @Override
    public void run() {
        try {
            lkpManager.refreshLkpFiles();
            //    lkpManager.closeFS();
        } catch (Exception e) {
            log.warn("[{}] refresh lkp file failed, Exception Details: [{}]",
                System.currentTimeMillis(), ExceptionUtils.getStackTrace(e));
        }

    }
}



