/**
 * 
 */
package com.immomo.rpc.ipc;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;

import javax.net.SocketFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.immomo.rpc.configuration.Configuration;
import com.immomo.rpc.io.ObjectWritable;
import com.immomo.rpc.io.Writable;
import com.immomo.rpc.ipc.Client.ConnectionId;
import com.immomo.rpc.ipc.RPC.RpcInvoker;
import com.immomo.rpc.util.MsgPackUtils;
import com.immomo.rpc.util.Time;

/**
 * 
 *
 */
public class MsgPackRPCEngine implements RPCEngine {

	private static final Log LOG = LogFactory.getLog(MsgPackRPCEngine.class);

	private static boolean isInitialized = false;
	static {
		ensureInitialized();
	}

	/**
	 * Initialize this class if it isn't already.
	 */
	public static synchronized void ensureInitialized() {
		if (!isInitialized) {
			initialize();
		}
	}

	/**
	 * Register the rpcRequest deserializer for WritableRpcEngine
	 */
	private static synchronized void initialize() {
		RPCServer.registerProtocolEngine(RpcKind.RPC_MSGPACK, Invocation.class,
				new Server.MsgPackRpcInvoker());
		isInitialized = true;
	}

	/***
	 * msgpackrpcengine server
	 * 
	 *
	 */
	public static class Server extends RPC.Server {

		public Server(String bindAddress, int port, Configuration conf,
				String serverName, int handlerCount) {
			super(bindAddress, port, conf, serverName, handlerCount);
		}

		public Server(Class<?> protocolClass, Object protocolImpl,
				Configuration conf, String bindAddress, int port,
				int numHandlers) throws IOException {
			this(bindAddress, port, conf, classNameBase(protocolImpl.getClass()
					.getName()), numHandlers);
			registerProtocolAndImpl(RpcKind.RPC_MSGPACK, protocolClass,
					protocolImpl);
		}

		static class MsgPackRpcInvoker implements RpcInvoker {

			@Override
			public Writable call(RPC.Server server, String protocolName,
					Writable rpcRequest, long receivedTime) throws Exception {
				Invocation call = (Invocation) rpcRequest;
				final String protoName;
				ProtoClassProtoImpl protocolImpl;
				protoName = call.declaringClassProtocolName;
				ProtoNameVer pv = new ProtoNameVer(
						call.declaringClassProtocolName);
				protocolImpl = server.getProtocolImplMap(RpcKind.RPC_MSGPACK)
						.get(pv);
				if (protocolImpl == null) { // no match for Protocol AND Version
					throw new Exception("Unknown protocol: " + protoName);
				}

				// Invoke the protocol method
				long startTime = Time.now();
				int qTime = (int) (startTime - receivedTime);
				Exception exception = null;
				try {
					Method method = protocolImpl.protocolClass.getMethod(
							call.getMethodName(), call.getParameterClasses());
					method.setAccessible(true);
					Object value = method.invoke(protocolImpl.protocolImpl,
							call.getParameters());
					return new ObjectWritable(method.getReturnType(), value);
				} catch (InvocationTargetException e) {
					Throwable target = e.getTargetException();
					if (target instanceof IOException) {
						exception = (IOException) target;
						throw (IOException) target;
					} else {
						IOException ioe = new IOException(target.toString());
						ioe.setStackTrace(target.getStackTrace());
						exception = ioe;
						throw ioe;
					}
				} catch (Throwable e) {
					if (!(e instanceof IOException)) {
						LOG.error("Unexpected throwable object ", e);
					}
					IOException ioe = new IOException(e.toString());
					ioe.setStackTrace(e.getStackTrace());
					exception = ioe;
					throw ioe;
				} finally {
					int processingTime = (int) (Time.now() - startTime);
					if (LOG.isDebugEnabled()) {
						String msg = "Served: " + call.getMethodName()
								+ " queueTime= " + qTime + " procesingTime= "
								+ processingTime;
						if (exception != null) {
							msg += " exception= "
									+ exception.getClass().getSimpleName();
						}
						LOG.debug(msg);
					}
					// Need statistic process Time
					// String detailedMetricsName = (exception == null) ? call
					// .getMethodName() : exception.getClass()
					// .getSimpleName();
				}
			}
		}

	}

	/***
	 * Invocation
	 */
	private static class Invocation implements Writable {
		private String methodName;
		private Class<?>[] parameterClasses;
		private Object[] parameters;
		private Configuration conf;
		private String declaringClassProtocolName;

		@SuppressWarnings("unused")
		// called when deserializing an invocation
		public Invocation() {
		}

		public Invocation(Method method, Object[] parameters) {
			this.methodName = method.getName();
			this.parameterClasses = method.getParameterTypes();
			this.parameters = parameters;
			this.declaringClassProtocolName = RPC.getProtocolName(method
					.getDeclaringClass());
		}

		/** The name of the method invoked. */
		public String getMethodName() {
			return methodName;
		}

		/** The parameter classes. */
		public Class<?>[] getParameterClasses() {
			return parameterClasses;
		}

		/** The parameter instances. */
		public Object[] getParameters() {
			return parameters;
		}

		@Override
		public void readFields(DataInput in) throws Exception {
			declaringClassProtocolName = MsgPackUtils.readString(in);
			methodName = MsgPackUtils.readString(in);
			parameters = new Object[MsgPackUtils.readInt(in)];
			parameterClasses = new Class[parameters.length];
			ObjectWritable objectWritable = new ObjectWritable();
			for (int i = 0; i < parameters.length; i++) {
				parameters[i] = ObjectWritable.readObject(in, objectWritable,
						this.conf);
				parameterClasses[i] = objectWritable.getDeclaredClass();
			}
		}

		@Override
		public void write(DataOutput out) throws IOException {
			MsgPackUtils.writeString(out, declaringClassProtocolName);
			MsgPackUtils.writeString(out, methodName);
			MsgPackUtils.writeInt(out, parameterClasses.length);
			for (int i = 0; i < parameterClasses.length; i++) {
				ObjectWritable.writeObject(out, parameters[i],
						parameterClasses[i], conf, true);
			}
		}

		@Override
		public String toString() {
			StringBuilder buffer = new StringBuilder();
			buffer.append(methodName);
			buffer.append("(");
			for (int i = 0; i < parameters.length; i++) {
				if (i != 0)
					buffer.append(", ");
				buffer.append(parameters[i]);
			}
			buffer.append(")");
			return buffer.toString();
		}

	}

	@Override
	public com.immomo.rpc.ipc.RPC.Server getServer(Class<?> protocol,
			Object instance, String bindAddress, int port, int numHandlers,
			Configuration conf) throws IOException {
		return new Server(protocol, instance, conf, bindAddress, port,
				numHandlers);
	}

	@Override
	public com.immomo.rpc.ipc.RPC.Server getServer(String bindAddress,
			int port, int numHandlers, Configuration conf, String serverName)
			throws IOException {
		return new Server(bindAddress, port, conf, serverName, numHandlers);
	}

	@Override
	@SuppressWarnings("unchecked")
	public <T> com.immomo.rpc.ipc.ProtocolProxy<T> getProxy(Class<T> protocol,
			String host, int port, Configuration conf, int rpcTimeout)
			throws IOException {
		T proxy = (T) Proxy.newProxyInstance(protocol.getClassLoader(),
				new Class[] { protocol }, new Invoker(protocol, host, port,
						conf, rpcTimeout));
		return new ProtocolProxy<T>(protocol, proxy);
	}

	private final static ClientCache CLIENTS = new ClientCache();

	private static class Invoker implements RpcInvocationHandler {
		private Client.ConnectionId remoteId;
		private Client client;
		private boolean isClosed = false;

		public Invoker(Class<?> protocol, String host, int port,
				Configuration conf, int rpcTimeout) throws IOException {
			this.remoteId = Client.ConnectionId.getConnectionId(host,port,
					protocol, rpcTimeout, conf);
			this.client = CLIENTS.getClient(conf,remoteId.toString());
		}
		

		@Override
		public Object invoke(Object proxy, Method method, Object[] args)
				throws Throwable {
			long startTime = 0;
			if (LOG.isDebugEnabled()) {
				startTime = Time.now();
			}
			ObjectWritable value;
			try {
				value = (ObjectWritable) client.call(RpcKind.RPC_MSGPACK,
						new Invocation(method, args), remoteId);
			} finally {
			}
			if (LOG.isDebugEnabled()) {
				long callTime = Time.now() - startTime;
				LOG.debug("Call: " + method.getName() + " " + callTime);
			}
			return value.get();
		}

		/* close the IPC client that's responsible for this invoker's RPCs */
		@Override
		synchronized public void close() {
			if (!isClosed) {
				isClosed = true;
				CLIENTS.stopClient(client);
			}
		}

		@Override
		public ConnectionId getConnectionId() {
			return remoteId;
		}
	}

}
