package com.nice.zookeeper;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author Luo Yong
 * @date 2017-03-12
 */
@Configuration
public class ZkConfig {

	@Value("${zookeeper.address}")
	private String address;
	@Value(("${zookeeper.sessionTimeoutMs:-1}"))
	private int sessionTimeoutMs;
	@Value("${zookeeper.connectionTimeoutMs:-1}")
	private int connectionTimeoutMs;
	@Value("${zookeeper.maxRetries:3}")
	private int maxRetries;
	@Value("${zookeeper.baseSleepTimeMs:1000}")
	private int baseSleepTimeMs;

	@Bean(initMethod = "init", destroyMethod = "stop")
	public ZkClient zkClient() {
		ZkClient zkClient = new ZkClient(this);
		return zkClient;
	}

	public String getAddress() {
		return address;
	}

	public int getSessionTimeoutMs() {
		return sessionTimeoutMs;
	}

	public int getConnectionTimeoutMs() {
		return connectionTimeoutMs;
	}

	public int getMaxRetries() {
		return maxRetries;
	}

	public int getBaseSleepTimeMs() {
		return baseSleepTimeMs;
	}
}
