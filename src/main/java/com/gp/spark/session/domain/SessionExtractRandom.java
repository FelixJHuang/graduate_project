package com.gp.spark.session.domain;

import java.io.Serializable;

/**
 * describe:
 *
 * @author huangjia
 * @date 2018/3/20
 */
public class SessionExtractRandom implements Serializable {
    private long task_id;
    private String session_id;
    private String start_time;
    private String catagroy_id;
    private String search_keywords;

    public long getTask_id() {
        return task_id;
    }

    public void setTask_id(long task_id) {
        this.task_id = task_id;
    }

    public String getSession_id() {
        return session_id;
    }

    public void setSession_id(String session_id) {
        this.session_id = session_id;
    }

    public String getStart_time() {
        return start_time;
    }

    public void setStart_time(String start_time) {
        this.start_time = start_time;
    }

    public String getCatagroy_id() {
        return catagroy_id;
    }

    public void setCatagroy_id(String catagroy_id) {
        this.catagroy_id = catagroy_id;
    }

    public String getSearch_keywords() {
        return search_keywords;
    }

    public void setSearch_keywords(String search_keywords) {
        this.search_keywords = search_keywords;
    }
}
