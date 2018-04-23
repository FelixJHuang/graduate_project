package com.gp.dao;

import com.gp.domain.TaskEntity;

/**
 * describe:
 *
 * @author huangjia
 * @date 2018/3/12
 */
public interface ITaskDAO {
    /**
     * 根据taskid查task
     *
     * @param taskId
     * @return
     */
    TaskEntity findById(long taskId);
}
