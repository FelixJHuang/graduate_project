package com.gp.dao;


import com.gp.domain.SessionAggrStat;

/**
 * session聚合统计模块DAO接口
 * @author Administrator
 *
 */
public interface ISessionDAO {

	/**
	 * 插入session聚合统计结果
	 * @param sessionAggrStat 
	 */
	void insert(SessionAggrStat sessionAggrStat);
	
}
