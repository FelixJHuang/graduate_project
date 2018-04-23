package com.gp.spark.realtime.domain;

import java.io.Serializable;

/**
 * describe:
 *
 * @author huangjia
 * @date 2018/3/23
 */
public class AdClickTrend implements Serializable {
    private String date;
    private String hour;
    private String minute;
    private long addid;
    private long clickCount;

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getHour() {
        return hour;
    }

    public void setHour(String hour) {
        this.hour = hour;
    }

    public String getMinute() {
        return minute;
    }

    public void setMinute(String minute) {
        this.minute = minute;
    }

    public long getAddid() {
        return addid;
    }

    public void setAddid(long addid) {
        this.addid = addid;
    }

    public long getClickCount() {
        return clickCount;
    }

    public void setClickCount(long clickCount) {
        this.clickCount = clickCount;
    }
}
