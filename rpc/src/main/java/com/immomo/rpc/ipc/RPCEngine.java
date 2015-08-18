/**
 * 
 */
package com.immomo.rpc.ipc;

import java.io.IOException;
import java.net.InetSocketAddress;

import javax.net.SocketFactory;

import com.immomo.rpc.configuration.Configuration;

/**
 * 
 *
 */
public interface RPCEngine {

	RPC.Server getServer(Class<?> protocol, Object instance,
			String bindAddress, int port, int numHandlers, Configuration conf)
			throws IOException;

	RPC.Server getServer(String bindAddress, int port, int numHandlers,
			Configuration conf, String serverName) throws IOException;

	<T> ProtocolProxy<T> getProxy(Class<T> protocol, String host,int port,
			Configuration conf, int rpcTimeout)
			throws IOException;

}
