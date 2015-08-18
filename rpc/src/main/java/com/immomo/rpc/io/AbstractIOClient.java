/**
 * 
 */
package com.immomo.rpc.io;

import com.immomo.rpc.configuration.Configuration;
import com.immomo.rpc.io.connection.IConnection;

/**
 * @author shuaipenghou 2015年6月17日下午4:55:43
 *
 */
public abstract class AbstractIOClient implements IIOClient {

	protected IPacketListener listener;

	private IConnection connection;

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.immomo.rpc.io.IIOClient#connect(java.lang.String, int)
	 */
	@Override
	public boolean connect(String host, int port, Configuration conf)
			throws Exception {
		return doConnect(host, port, conf);
	}

	public abstract boolean doConnect(String host, int port, Configuration conf)
			throws Exception;

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.immomo.rpc.io.IIOClient#disconnect()
	 */
	@Override
	public boolean disconnect() {
		// TODO Auto-generated method stub
		return doDisconnect();
	}

	public abstract boolean doDisconnect();

	@Override
	public IIOClient registerListener(IPacketListener listener) {
		this.listener = listener;
		return this;
	}

	@Override
	public IPacketListener getListener() {
		return listener;
	}

	@Override
	public IConnection getConnection() {
		return this.connection;
	}

	@Override
	public void setConnection(IConnection conn) {
		this.connection = conn;
	}

}
