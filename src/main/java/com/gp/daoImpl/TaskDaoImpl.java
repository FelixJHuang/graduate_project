package com.gp.daoImpl;

import com.gp.dao.ITaskDAO;
import com.gp.domain.TaskEntity;
import com.gp.helper.JDBCManager;

import java.sql.ResultSet;

/**
 * describe:
 *
 * @author huangjia
 * @date 2018/3/12
 */
public class TaskDaoImpl implements ITaskDAO {

    @Override
    public TaskEntity findById(final long taskId) {

        final TaskEntity taskEntity = new TaskEntity();

        String sql = "select * from task where task_id=?";
        JDBCManager instance = JDBCManager.getInstance();
        instance.exexuteQuery(sql, new Object[]{taskId}, new JDBCManager.QueryCallBack() {
            @Override
            public void process(ResultSet rs) throws Exception {
                //将查询到的结果封装成实体类
                if (rs.next()) {
                    long taskid = rs.getLong(1);
                    String taskName = rs.getString(2);
                    String createTime = rs.getString(3);
                    String startTime = rs.getString(4);
                    String finishTime = rs.getString(5);
                    String taskType = rs.getString(6);
                    String taskStatus = rs.getString(7);
                    String taskParams = rs.getString(8);

                    //封装信息
                    taskEntity.setTaskId(taskid);
                    taskEntity.setCreateTime(createTime);
                    taskEntity.setFinishTime(finishTime);
                    taskEntity.setStartTime(startTime);
                    taskEntity.setTaskName(taskName);
                    taskEntity.setTaskParams(taskParams);
                    taskEntity.setTaskStatus(taskStatus);
                    taskEntity.setTaskType(taskType);
                }
            }
        });

        return taskEntity;
    }
}
