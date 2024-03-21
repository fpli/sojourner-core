package com.ebay.sojourner.cjs.util;

import com.ebay.sojourner.cjs.service.CjsMetadataManager;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.util.TimerTask;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Slf4j
public class CjsLkpRefreshTimeTask extends TimerTask {

    private final CjsMetadataManager cjsMetadataManager;

    public CjsLkpRefreshTimeTask(CjsMetadataManager cjsMetadataManager, TimeUnit timeUnit) {
        this.cjsMetadataManager = cjsMetadataManager;
        //Set start at 00:05:00 every day
        //        Calendar calendar = Calendar.getInstance();
        //        calendar.set(Calendar.HOUR_OF_DAY, 0);
        //        calendar.set(Calendar.MINUTE, 5);
        //        calendar.set(Calendar.SECOND, 0);
        //        Timer timer = new Timer(true);
        //        timer.scheduleAtFixedRate(this, calendar.getTime(), timeUnit.toMillis(1));
        ScheduledThreadPoolExecutor scheduledThreadPoolExecutor =
                new ScheduledThreadPoolExecutor(1, r -> {
                    Thread t = new Thread(r, "soj-cjs-lkp-refresh-thread");
                    t.setDaemon(true);
                    return t;
                });
        scheduledThreadPoolExecutor
                .scheduleAtFixedRate(this, 1, 1, timeUnit);
    }

    @Override
    public void run() {
        try {
            cjsMetadataManager.refresh();
            //    lkpManager.closeFS();
        } catch (Exception e) {
            log.warn("[{}] refresh lkp file failed, Exception Details: [{}]",
                     System.currentTimeMillis(), ExceptionUtils.getStackTrace(e));
        }

    }
}
