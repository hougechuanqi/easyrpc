/**
 * 
 */
package com.immomo.rpc.ipc;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import javax.net.SocketFactory;

import com.google.common.collect.Maps;
import com.immomo.rpc.annotation.ProtocolInfo;
import com.immomo.rpc.configuration.ConfigType;
import com.immomo.rpc.configuration.Configuration;
import com.immomo.rpc.configuration.ConfigurationKeys;
import com.immomo.rpc.exception.RPCParamterErrorException;
import com.immomo.rpc.io.Writable;
import com.immomo.rpc.util.NetUtils;
import com.immomo.rpc.util.ReflectionUtils;

/**
 * RPC
 *
 */
public class RPC {

	private static final Map<Class<?>, RPCEngine> PROTOCOL_ENGINES = new HashMap<Class<?>, RPCEngine>();

	private static final String ENGINE_PROP = "rpc.engine";

	private static final String RPC_SERVER = "rpcserver";

	private static final RpcKind DEFAULT_RPCKIND = RpcKind.RPC_MSGPACK;
	

	/***
	 * Server builder
	 */
	public static class Builder {
		private Map<Class<?>, Object> protocalMap = Maps.newHashMap();
		private String bindAddress = "0.0.0.0";
		private int port = 0;
		private int numHandlers = 1;
		private final Configuration conf;

		public Builder(Configuration conf) {
			this.conf = conf;
		}

		public Builder addProtocol(Class<?> protocol, Object object) {
			protocalMap.put(protocol, object);
			return this;
		}

		/** Default: 0.0.0.0 */
		public Builder setBindAddress(String bindAddress) {
			this.bindAddress = bindAddress;
			return this;
		}

		/** Default: 0 */
		public Builder setPort(int port) {
			this.port = port;
			return this;
		}

		/** Default: 1 */
		public Builder setNumHandlers(int numHandlers) {
			this.numHandlers = numHandlers;
			return this;
		}

		public Server build() throws IOException, RPCParamterErrorException {
			if (this.conf == null) {
				throw new RPCParamterErrorException("conf is not set");
			}
			if (this.protocalMap.size() <= 0) {
				throw new RPCParamterErrorException("protocol is not set");
			}
			RPCEngine rpcEngine = null;
			for (Class<?> clazz : protocalMap.keySet()) {
				rpcEngine = getProtocolEngine(clazz, conf);
			}
			if (port <= 0 && conf.getConfig(ConfigType.PORT) != null) {
				port = conf.getConfig(ConfigType.PORT);
			}
			Server server = rpcEngine.getServer(bindAddress, port, numHandlers,
					conf, RPC_SERVER);
			for (Class<?> clazz : protocalMap.keySet()) {
				server.registerProtocolAndImpl(DEFAULT_RPCKIND, clazz,
						protocalMap.get(clazz));
			}
			return server;
		}
	}

	// return the RpcEngine configured to handle a protocol
	static synchronized RPCEngine getProtocolEngine(Class<?> protocol,
			Configuration conf) {
		RPCEngine engine = PROTOCOL_ENGINES.get(protocol);
		if (engine == null) {
			engine = (RPCEngine) ReflectionUtils.newInstance(
					MsgPackRPCEngine.class, conf);
			PROTOCOL_ENGINES.put(protocol, engine);
		}
		return engine;
	}

	interface RpcInvoker {
		public Writable call(Server server, String protocol,
				Writable rpcRequest, long receiveTime) throws Exception;
	}

	static public String getProtocolName(Class<?> protocol) {
		if (protocol == null) {
			return null;
		}
		ProtocolInfo anno = protocol.getAnnotation(ProtocolInfo.class);
		return (anno == null) ? protocol.getName() : anno.protocolName();
	}

	public static abstract class Server extends RPCServer {

		protected Server(String bindAddress, int port, Configuration conf,
				String serverName, int handlerCount) {
			super(bindAddress, port, conf, serverName, handlerCount);
		}

		protected ArrayList<Map<ProtoNameVer, ProtoClassProtoImpl>> protocolImplMapArray = new ArrayList<Map<ProtoNameVer, ProtoClassProtoImpl>>(
				RpcKind.MAX_INDEX);

		@Override
		public Writable call(RpcKind rpcKind, String protocol,
				Writable rpcRequest, long receiveTime) throws Exception {
			return getRpcInvoker(rpcKind).call(this, protocol, rpcRequest,
					receiveTime);
		}

		public Server addProtocol(RpcKind rpcKind, Class<?> protocolClass,
				Object protocolImpl) {
			registerProtocolAndImpl(rpcKind, protocolClass, protocolImpl);
			return this;
		}

		public static String classNameBase(String className) {
			String[] names = className.split("\\.", -1);
			if (names == null || names.length == 0) {
				return className;
			}
			return names[names.length - 1];
		}

		public void registerProtocolAndImpl(RpcKind rpcKind,
				Class<?> protocolClass, Object protocolImpl) {
			String protocolName = RPC.getProtocolName(protocolClass);
			getProtocolImplMap(rpcKind).put(new ProtoNameVer(protocolName),
					new ProtoClassProtoImpl(protocolClass, protocolImpl));
		}

		Map<ProtoNameVer, ProtoClassProtoImpl> getProtocolImplMap(
				RpcKind rpcKind) {
			if (protocolImplMapArray.size() == 0) {// initialize for all rpc
													// kinds
				for (int i = 0; i <= RpcKind.MAX_INDEX; ++i) {
					protocolImplMapArray
							.add(new HashMap<ProtoNameVer, ProtoClassProtoImpl>(
									10));
				}
			}
			return protocolImplMapArray.get(rpcKind.ordinal());
		}

		static class ProtoClassProtoImpl {
			final Class<?> protocolClass;
			final Object protocolImpl;

			ProtoClassProtoImpl(Class<?> protocolClass, Object protocolImpl) {
				this.protocolClass = protocolClass;
				this.protocolImpl = protocolImpl;
			}
		}

		/***
		 * protocal name wrapper
		 */
		static class ProtoNameVer {
			final String protocol;

			ProtoNameVer(String protocol) {
				this.protocol = protocol;
			}

			@Override
			public boolean equals(Object o) {
				if (o == null)
					return false;
				if (this == o)
					return true;
				if (!(o instanceof ProtoNameVer))
					return false;
				ProtoNameVer pv = (ProtoNameVer) o;
				return ((pv.protocol.equals(this.protocol)));
			}

			@Override
			public int hashCode() {
				return protocol.hashCode() * 37;
			}
		}

	}

	public static <T> T getProxy(Class<T> protocol, String host, int port,
			Configuration conf, int rpcTimeout) throws IOException {

		return getProtocolProxy(protocol, host, port, conf, rpcTimeout)
				.getProxy();
	}

	public static <T> T getProxy(Class<T> protocol, String host, int port,
			Configuration conf) throws IOException {

		return getProtocolProxy(
				protocol,
				host,
				port,
				conf,
				conf.getInt(ConfigType.IPC_PROC_TIMEOUT,
						ConfigurationKeys.IPC_PROC_TIMEOUT)).getProxy();
	}

	public static <T> ProtocolProxy<T> getProtocolProxy(Class<T> protocol,
			String host, int port, Configuration conf, int rpcTimeout)
			throws IOException {
		return getProtocolEngine(protocol, conf).getProxy(protocol, host, port,
				conf, rpcTimeout);
	}
}
