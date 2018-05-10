package com.nice.zookeeper;

/**
 * ZK 连接状态监听
 * @author Luo Yong
 * @date 2017-03-12
 */
public interface ZkStateListener {

	/**
	 * 重连调用方法
	 */
	void reconnected();
}
