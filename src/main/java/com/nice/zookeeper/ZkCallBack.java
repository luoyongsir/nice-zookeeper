package com.nice.zookeeper;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.utils.ZKPaths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

/**
 * 1.监控一个ZNode或者ZNode的子节点的回调函数
 * 2.选择一个 leader 后回调执行任务
 *
 * @author Luo Yong
 * @date 2017-03-12
 */
public interface ZkCallBack {

	Logger LOG = LoggerFactory.getLogger(ZkCallBack.class.getName());

	/**
	 * 监控一个ZNode. 当节点的数据修改或者删除时的回调函数
	 *
	 * @param curator
	 * @param cache 
	 *            得到节点当前的状态：cache.getCurrentData()；得到当前的值：cache.getCurrentData().getData()
	 */
	default void listenerNode(CuratorFramework curator, NodeCache cache) {
		// 默认打印数据
		if (cache.getCurrentData() != null) {
			LOG.info("Node changed: " + cache.getCurrentData().getPath() + ", value: "
					+ new String(cache.getCurrentData().getData(), StandardCharsets.UTF_8));
		}
	}

	/**
	 * 监控一个ZNode的子节点。当一个子节点增加， 更新，删除时时的回调函数
	 *
	 * @param curator
	 * @param event
	 */
	default void listenerChildNode(CuratorFramework curator, PathChildrenCacheEvent event) {
		String path = ZKPaths.getNodeFromPath(event.getData().getPath());
		String data = new String(event.getData().getData(), StandardCharsets.UTF_8);
		switch (event.getType()) {
			case CHILD_ADDED:
				LOG.info("Node added: " + path + " data " + data);
				break;
			case CHILD_UPDATED:
				LOG.info("Node changed: " + path + " data " + data);
				break;
			case CHILD_REMOVED:
				LOG.info("Node removed: " + path + " data " + data);
				break;
			default:
				LOG.info(event.toString() + path + " data " + data);
				break;
		}
	}

	/**
	 * 选择一个 leader 后回调执行任务
	 *
	 * @param curator
	 * @param obj
	 */
	default void execTask(CuratorFramework curator, Object... obj) {
		LOG.info(" Become a leader");
		LOG.info(" Do the task…… ");
		LOG.info(" Complete the task and become non-leader ");
	}
}
