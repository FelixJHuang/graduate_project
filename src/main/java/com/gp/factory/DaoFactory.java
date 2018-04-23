package com.gp.factory;

import com.gp.dao.*;
import com.gp.daoImpl.*;

/**
 * describe: dao工厂
 *
 * @author huangjia
 * @date 2018/3/12
 */
public class DaoFactory {
    /**
     * 获取taskDao的工厂
     *
     * @return
     */
    public static ITaskDAO getTaskDao() {
        return new TaskDaoImpl();
    }

    public static ISessionDAO getSessionAggrStatDAO() {
        return new SessionImpl();
    }

    public static IRandExtactSessionDao getRandExtractSession() {
        return new RandExtractSession();
    }

    public static ISessionDetailDAO getSessionDetail() {
        return new SessionDetailDAO();
    }

    public static ITop10DAO getItop10() {
        return new Top10DaoImpl();
    }

    public static ClickCountDao getClickDao() {
        return new ClickCountDaoImpl();
    }

    public static IAdBlacklistDAO getAdBlacklistDAO() {
        return new AdBlacklistDAOImpl();
    }
    public static IAdClickTrendDAO getIAdClickTrendDAO(){
        return new AdClickTrendDaoImpl();
    }
}
