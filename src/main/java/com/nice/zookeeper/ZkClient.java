package com.nice.zookeeper;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.CuratorFrameworkFactory.Builder;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Zookeeper操作工具类
 *
 * @author Luo Yong
 * @date 2017-03-12
 */
public class ZkClient {

	private static final Logger LOG = LoggerFactory.getLogger(ZkClient.class.getName());
	private ZkConfig zkConfig;
	private CuratorFramework curator;

	public ZkClient(ZkConfig zkConfig) {
		this.zkConfig = zkConfig;
	}

	public void init() {
		RetryPolicy retryPolicy = new ExponentialBackoffRetry(zkConfig.getBaseSleepTimeMs(), zkConfig.getMaxRetries());
		Builder builder = CuratorFrameworkFactory.builder().connectString(zkConfig.getAddress());
		if (zkConfig.getSessionTimeoutMs() != -1) {
			builder.sessionTimeoutMs(zkConfig.getSessionTimeoutMs());
		}
		if (zkConfig.getConnectionTimeoutMs() != -1) {
			builder.connectionTimeoutMs(zkConfig.getConnectionTimeoutMs());
		}
		curator = builder.retryPolicy(retryPolicy).build();
		// 连接状态监听
		curator.getConnectionStateListenable().addListener((client, newState) -> {
			if (newState == ConnectionState.CONNECTED) {
				// 连接新建
				LOG.warn(" connected with zookeeper ");
			} else if (newState == ConnectionState.LOST) {
				// 连接丢失
				LOG.warn(" lost session with zookeeper ");
			} else if (newState == ConnectionState.RECONNECTED) {
				LOG.warn("reconnected with zookeeper");
				// 连接重连
				for (ZkStateListener s : stateListeners) {
					s.reconnected();
				}
			}
		});
		curator.start();
	}

	public void stop() {
		curator.close();
	}

	public CuratorFramework getCurator() {
		return curator;
	}

	/**
	 * 给节点设置值，如果节点不存在，则自动创建持久化节点
	 *
	 * @param path 节点路径
	 * @param data
	 * @return
	 */
	public String setData(String path, String data) {
		return setData(path, data, null);
	}

	/**
	 * 给节点设置值，如果节点不存在，则自动创建 mode 类型（默认为持久化类型）的节点
	 *
	 * @param path 节点路径
	 * @param data 节点数据
	 * @param mode
	 * @return
	 */
	public String setData(String path, String data, CreateMode mode) {
		if (StringUtils.isEmpty(path) || StringUtils.isEmpty(data)) {
			return null;
		}
		// 如果mode为空则默认为持久化节点
		if (mode == null) {
			mode = CreateMode.PERSISTENT;
		}
		return setData(path, data.getBytes(StandardCharsets.UTF_8), mode);
	}

	/**
	 * @param path 节点路径
	 * @param data 节点数据
	 * @param mode
	 * @return 返回真正写到的路径，如果出错则返回null
	 */
	public String setData(String path, byte[] data, CreateMode mode) {
		String zkPath = path;
		try {
			if (curator.checkExists().forPath(path) == null) {
				zkPath = curator.create().creatingParentsIfNeeded().withMode(mode).forPath(path, data);
			} else {
				curator.setData().forPath(path, data);
			}
			return zkPath;
		} catch (Exception e) {
			LOG.error("zookeeper set data [{}] with path [{}] error: ", data, path, e);
			return null;
		}
	}

	/**
	 * 获取节点数据
	 *
	 * @param path 节点路径
	 * @return
	 */
	public String getData(String path) {
		try {
			byte[] bytes = curator.getData().forPath(path);
			if (bytes != null && bytes.length > 0) {
				return new String(bytes, StandardCharsets.UTF_8);
			}
		} catch (Exception e) {
			LOG.error("获取数据出错：", e);
		}
		return null;
	}

	/**
	 * 获取指定节点下的子节点列表
	 *
	 * @param path 节点路径
	 * @return
	 */
	public List<String> getChildren(String path) {
		try {
			if (exists(path)) {
				return curator.getChildren().forPath(path);
			}
		} catch (Exception e) {
			LOG.error("获取子节点出错", e);
		}
		return new ArrayList<>();
	}

	/**
	 * 删除节点，并递归删除其所有子节点
	 *
	 * @param path 节点路径
	 */
	public void delete(String path) {
		delete(path, false);
	}

	/**
	 * 删除节点，并递归删除其所有子节点
	 *
	 * @param path  节点路径
	 * @param guaranteed 是否保证删除
	 */
	public void delete(String path, boolean guaranteed) {
		try {
			if (guaranteed) {
				curator.delete().guaranteed().deletingChildrenIfNeeded().forPath(path);
			} else {
				curator.delete().deletingChildrenIfNeeded().forPath(path);
			}
		} catch (Exception e) {
			LOG.error("删除节点出错", e);
		}
	}

	/**
	 * 判断节点是否存在
	 *
	 * @param path 节点路径
	 * @return
	 * @throws Exception
	 */
	public boolean exists(String path) throws Exception {
		return path != null && curator.checkExists().forPath(path) != null;
	}

	/**
	 * 监听节点数据变化，节点数据变化时，执行ZkListenerCallBack回调方法
	 *
	 * @param path     节点，不能为空
	 * @param callBack 内容变化后回调 listenerNode 方法，不能为空
	 */
	public void addNodeListener(final String path, final ZkCallBack callBack) {
		try {
			if (!exists(path) || callBack == null) {
				return;
			}
			final NodeCache cache = new NodeCache(curator, path);
			cache.start(true);
			cache.getListenable().addListener(() -> callBack.listenerNode(curator, cache));
		} catch (Exception e) {
			LOG.error("注册节点 {} 监听失败：", path, e);
		}
	}

	/**
	 * 监听子节点变化，子节点变化时，执行ZkListenerCallBack回调方法
	 *
	 * @param path     节点，不能为空
	 * @param callBack 子节点变化时回调 listenerChildNode 方法，不能为空
	 */
	public void addChildNodeListener(final String path, final ZkCallBack callBack) {
		try {
			if (!exists(path) || callBack == null) {
				return;
			}
			final PathChildrenCache cache = new PathChildrenCache(curator, path, true);
			cache.start();
			cache.getListenable().addListener((client, event) -> callBack.listenerChildNode(client, event));
		} catch (Exception e) {
			LOG.error("注册节点 {} 的子节点监听失败：", path, e);
		}
	}

	/**
	 * 在 path 节点下选择一个 leader 执行任务，任务完成后释放 leader
	 *
	 * @param path     节点，不能为空
	 * @param callBack 子节点变化时回调 listenerChildNode 方法，不能为空
	 * @param obj      回调方法 execTask 的参数，可以为空
	 */
	public void takeLeaderAndExecTask(final String path, final ZkCallBack callBack, Object... obj) {
		try {
			if (!exists(path) || callBack == null) {
				return;
			}
		} catch (Exception e) {
			LOG.error("注册节点 {} 的子节点监听失败，参数错误：", path, e);
		}
		LeaderSelectorListenerAdapter listener = new LeaderSelectorListenerAdapter() {

			@Override
			public void takeLeadership(CuratorFramework curatorFramework) {
				callBack.execTask(curatorFramework, obj);
			}
		};
		try (LeaderSelector leader = new LeaderSelector(curator, path, listener)) {
			leader.autoRequeue();
			leader.start();
		}
	}


	private final List<ZkStateListener> stateListeners = new ArrayList<>();

	public void addStateListener(ZkStateListener listener) {
		stateListeners.add(listener);
	}
}
