
package com.nice.zookeeper;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.NodeCache;

import java.util.Map;

/**
 * 节点的数据修改或者删除时，回调方法。
 * @author Luo Yong
 * @date 2017-03-12
 */
public interface ZkNodeChangeCallBack {

	/**
	 * 节点内容变化后的回调函数
	 * @param curator
	 * @param cache 
	 *            得到节点当前的状态：cache.getCurrentData()；得到当前的值：cache.getCurrentData().getData()
	 * @param params 回调函数的额外参数，可以为空
	 */
	void execute(CuratorFramework curator, NodeCache cache, Map<String, Object> params);
}
