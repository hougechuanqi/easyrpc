/**
 * 
 */
package com.immomo.rpc.io.connection;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicLong;

import com.immomo.rpc.io.msg.Packet;
import com.immomo.rpc.ipc.RPCServer.Call;

/**
 * 
 *
 */
public abstract class AbstractConneciton implements IConnection {

	protected long lastContact;

	protected AtomicLong rpctimes = new AtomicLong(0);
	
	/*
	 * (non-Javadoc)
	 * 
	 * @see com.immomo.rpc.io.connection.IConnection#getRemoteAddress()
	 */
	@Override
	public InetSocketAddress getRemoteAddress() {
		return doGetRemoteAddress();
	}

	public abstract InetSocketAddress doGetRemoteAddress();

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.immomo.rpc.io.connection.IConnection#getSessionId()
	 */
	@Override
	public String getSessionId() {
		return doGetStringId();
	}

	public abstract String doGetStringId();

	@Override
	public boolean send(Packet packet) {
		return doSend(packet);
	}

	public abstract boolean doSend(Packet packet);

	@Override
	public boolean close() {
		return doClose();
	}

	public abstract boolean doClose();

	@Override
	public long getLastContact() {
		return this.lastContact;
	}

	@Override
	public boolean isIdle() {
		return rpctimes.get() == 0;
	}

	@Override
	public boolean isAlive() {

		return doIsAlive();
	}

	@Override
	public void doResponse(Call call) {
		response(call);
	}
	
	public abstract void response(Call call);

	protected abstract boolean doIsAlive();


}
