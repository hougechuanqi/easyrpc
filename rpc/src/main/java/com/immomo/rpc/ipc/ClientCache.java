package com.immomo.rpc.ipc;

import java.util.HashMap;
import java.util.Map;

import com.immomo.rpc.configuration.Configuration;
import com.immomo.rpc.io.ObjectWritable;
import com.immomo.rpc.io.Writable;
import com.immomo.rpc.util.Util;

public class ClientCache {
	private Map<String, Client> clients = new HashMap<String, Client>();

	/**
	 * Construct & cache an IPC client with the user-provided SocketFactory if
	 * no cached client exists.
	 * 
	 * @param conf
	 *            Configuration
	 * @param factory
	 *            SocketFactory for client socket
	 * @param valueClass
	 *            Class of the expected response
	 * @return an IPC client
	 */
	public synchronized Client getClient(Configuration conf, String remoteId,
			Class<? extends Writable> valueClass) {
		Client client = clients.get(remoteId);
		if (client == null) {
			client = new Client(valueClass, conf, remoteId);
			clients.put(remoteId, client);
		} else {
			client.incCount();
		}
		if (Client.LOG.isDebugEnabled()) {
			Client.LOG.debug("getting client out of cache: " + remoteId);
		}
		return client;
	}

	/**
	 * Construct & cache an IPC client with the default SocketFactory and
	 * default valueClass if no cached client exists.
	 * 
	 * @param conf
	 *            Configuration
	 * @return an IPC client
	 */
	public synchronized Client getClient(Configuration conf,String remoteId) {
		return getClient(conf, remoteId, ObjectWritable.class);
	}

	/**
	 * Stop a RPC client connection A RPC client is closed only when its
	 * reference count becomes zero.
	 */
	public void stopClient(Client client) {
		if (Client.LOG.isDebugEnabled()) {
			Client.LOG.debug("stopping client from cache: " + client);
		}
		synchronized (this) {
			client.decCount();
			if (client.isZeroReference()) {
				if (Client.LOG.isDebugEnabled()) {
					Client.LOG.debug("removing client from cache: " + client);
				}
				clients.remove(client.getClientId());
			}
		}
		if (client.isZeroReference()) {
			if (Client.LOG.isDebugEnabled()) {
				Client.LOG
						.debug("stopping actual client because no more references remain: "
								+ client);
			}
			client.stop();
		}
	}
}
