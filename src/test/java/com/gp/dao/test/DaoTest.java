package com.gp.dao.test;

import com.gp.dao.ITaskDAO;
import com.gp.domain.TaskEntity;
import com.gp.factory.DaoFactory;

/**
 * describe: 测试taskDao
 *
 * @author huangjia
 * @date 2018/3/12
 */
public class DaoTest {
    public static void main(String[] args) {
        ITaskDAO taskDao = DaoFactory.getTaskDao();
        TaskEntity taskEntity = taskDao.findById(1l);
        System.out.println(taskEntity.getTaskStatus());
    }
}
