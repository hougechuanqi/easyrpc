package com.immomo.rpc.ipc;

import java.io.Closeable;
import java.lang.reflect.InvocationHandler;

import com.immomo.rpc.ipc.Client.ConnectionId;

public interface RpcInvocationHandler extends InvocationHandler, Closeable {

	/**
	 * Returns the connection id associated with the InvocationHandler instance.
	 * 
	 * @return ConnectionId
	 */
	ConnectionId getConnectionId();
}