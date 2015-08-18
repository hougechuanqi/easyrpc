/**
 * 
 */
package com.immomo.rpc.ipc;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.immomo.rpc.configuration.ConfigType;
import com.immomo.rpc.configuration.Configuration;
import com.immomo.rpc.configuration.ConfigurationKeys;
import com.immomo.rpc.exception.RemoteException;
import com.immomo.rpc.io.DataOutputBuffer;
import com.immomo.rpc.io.IPacketListener;
import com.immomo.rpc.io.PacketWrapper;
import com.immomo.rpc.io.Writable;
import com.immomo.rpc.io.client.netty.IONettyClient;
import com.immomo.rpc.io.connection.IConnection;
import com.immomo.rpc.io.msg.Body;
import com.immomo.rpc.io.msg.Header;
import com.immomo.rpc.io.msg.Packet;
import com.immomo.rpc.ipc.Client.ConnectionId;
import com.immomo.rpc.ipc.msgpack.MsgType;
import com.immomo.rpc.ipc.msgpack.MsgUtil;
import com.immomo.rpc.ipc.msgpack.RPCRequestHeader;
import com.immomo.rpc.ipc.msgpack.RPCResponseHeader;
import com.immomo.rpc.ipc.msgpack.RPCStatus;
import com.immomo.rpc.util.IOUtils;
import com.immomo.rpc.util.MsgPackUtils;
import com.immomo.rpc.util.NetUtils;
import com.immomo.rpc.util.ReflectionUtils;
import com.immomo.rpc.util.Time;
import com.immomo.rpc.util.Util;

/**
 * @author shuaipenghou 2015骞�6鏈�8鏃ヤ笅鍗�6:56:54
 *
 */
public class Client {

	public static final Log LOG = LogFactory.getLog(Client.class);

	private static final ThreadLocal<String> callId = new ThreadLocal<String>();

	public static void setCallId(String cid) {
		callId.set(cid);
	}

	private Hashtable<ConnectionId, Connection> connections = new Hashtable<ConnectionId, Connection>();

	private Class<? extends Writable> valueClass; // class of call values
	private AtomicBoolean running = new AtomicBoolean(true); // if client runs
	final private Configuration conf;

	private int refCount = 1;

	private final String clientId;

	public String getClientId() {

		return this.clientId;
	}

	/**
	 * Executor on which IPC calls' parameters are sent. Deferring the sending
	 * of parameters to a separate thread isolates them from thread
	 * interruptions in the calling code.
	 */
	private final ExecutorService sendParamsExecutor;
	private final static ClientExecutorServiceFactory clientExcecutorFactory = new ClientExecutorServiceFactory();

	private static class ClientExecutorServiceFactory {
		private int executorRefCount = 0;
		private ExecutorService clientExecutor = null;

		/**
		 * Get Executor on which IPC calls' parameters are sent. If the internal
		 * reference counter is zero, this method creates the instance of
		 * Executor. If not, this method just returns the reference of
		 * clientExecutor.
		 * 
		 * @return An ExecutorService instance
		 */
		synchronized ExecutorService refAndGetInstance() {
			if (executorRefCount == 0) {
				clientExecutor = Executors
						.newCachedThreadPool(new ThreadFactoryBuilder()
								.setDaemon(true)
								.setNameFormat(
										"IPC Parameter Sending Thread #%d")
								.build());
			}
			executorRefCount++;

			return clientExecutor;
		}

		/**
		 * Cleanup Executor on which IPC calls' parameters are sent. If
		 * reference counter is zero, this method discards the instance of the
		 * Executor. If not, this method just decrements the internal reference
		 * counter.
		 * 
		 * @return An ExecutorService instance if it exists. Null is returned if
		 *         not.
		 */
		synchronized ExecutorService unrefAndCleanup() {
			executorRefCount--;
			assert (executorRefCount >= 0);

			if (executorRefCount == 0) {
				clientExecutor.shutdown();
				try {
					if (!clientExecutor.awaitTermination(1, TimeUnit.MINUTES)) {
						clientExecutor.shutdownNow();
					}
				} catch (InterruptedException e) {
					LOG.error("Interrupted while waiting for clientExecutor"
							+ "to stop", e);
					clientExecutor.shutdownNow();
				}
				clientExecutor = null;
			}
			return clientExecutor;
		}
	};

	/**
	 * set the ping interval value in configuration
	 * 
	 * @param conf
	 *            Configuration
	 * @param pingInterval
	 *            the ping interval
	 */
	final public static void setPingInterval(Configuration conf,
			int pingInterval) {
		conf.addConfigDefault(ConfigType.PING_INTERVAL,
				ConfigurationKeys.IPC_PING_INTERVAL_DEFAULT, pingInterval);
	}

	/**
	 * Get the ping interval from configuration; If not set in the
	 * configuration, return the default value.
	 * 
	 * @param conf
	 *            Configuration
	 * @return the ping interval
	 */
	final public static int getPingInterval(Configuration conf) {
		return (Integer) conf.getConfig(ConfigType.PING_INTERVAL,
				ConfigurationKeys.IPC_PING_INTERVAL_DEFAULT);
	}

	final public static int getTimeout(Configuration conf) {
		return (Integer) conf.getConfig(ConfigType.CALL_TIMEOUT,
				ConfigurationKeys.IPC_CALL_TIMEOUT);
	}

	/**
	 * set the connection timeout value in configuration
	 * 
	 * @param conf
	 *            Configuration
	 * @param timeout
	 *            the socket connect timeout value
	 */
	public static final void setConnectTimeout(Configuration conf, int timeout) {
		conf.addConfigDefault(ConfigType.CONNECTION_TIMEOUT, timeout,
				ConfigurationKeys.IPC_CONNECTION_TIMEOUT);
	}

	/**
	 * Increment this client's reference count
	 *
	 */
	synchronized void incCount() {
		refCount++;
	}

	/**
	 * Decrement this client's reference count
	 *
	 */
	synchronized void decCount() {
		refCount--;
	}

	/**
	 * Return if this client has no reference
	 * 
	 * @return true if this client has no reference; false otherwise
	 */
	synchronized boolean isZeroReference() {
		return refCount == 0;
	}

	Call createCall(RpcKind rpcKind, Writable rpcRequest, String protoName) {
		return new Call(rpcKind, rpcRequest, protoName);
	}

	/**
	 * Class that represents an RPC call
	 */
	static class Call {
		final String id; // call id
		final Writable rpcRequest; // the serialized rpc request
		Writable rpcResponse; // null if rpc has error
		IOException error; // exception, null if success
		final RpcKind rpcKind; // Rpc EngineKind
		boolean done; // true when call is done
		private String protoName;

		private Call(RpcKind rpcKind, Writable param, String protoName) {
			this.rpcKind = rpcKind;
			this.rpcRequest = param;
			this.protoName = protoName;
			final String id = callId.get();
			if (id == null) {
				this.id = nextCallId();
			} else {
				callId.set(null);
				this.id = id;
			}
		}

		/**
		 * Indicate when the call is complete and the value or error are
		 * available. Notifies by default.
		 */
		protected synchronized void callComplete() {
			this.done = true;
			notify(); // notify caller
		}

		/**
		 * Set the exception when there is an error. Notify the caller the call
		 * is done.
		 * 
		 * @param error
		 *            exception thrown by the call; either local or remote
		 */
		public synchronized void setException(IOException error) {
			this.error = error;
			callComplete();
		}

		/**
		 * Set the return value when there is no error. Notify the caller the
		 * call is done.
		 * 
		 * @param rpcResponse
		 *            return value of the rpc call.
		 */
		public synchronized void setRpcResponse(Writable rpcResponse) {
			this.rpcResponse = rpcResponse;
			callComplete();
		}

		public synchronized Writable getRpcResponse() {
			return rpcResponse;
		}

	}

	/**
	 * Thread that reads responses and notifies callers. Each connection owns a
	 * socket connected to a remote address. Calls are multiplexed through this
	 * socket: responses may be delivered out of order.
	 */
	private class Connection {
		private String host;
		private int port;
		private final ConnectionId remoteId; // connection id
		private int rpcTimeout;
		private int maxIdleTime; // connections will be culled if it was idle
		private boolean doPing; // do we need to send ping message
		private int pingInterval; // how often sends ping to the server in msecs
		private Packet pingRequest; // ping message
		private Hashtable<String, Call> calls = new Hashtable<String, Call>();
		private AtomicLong lastActivity = new AtomicLong();// last I/O activity
															// time
		private AtomicBoolean shouldCloseConnection = new AtomicBoolean(); // indicate
		private IOException closeException; // close reason
		private IONettyClient client;
		private final Object sendRpcRequestLock = new Object();
		private IConnection conn;
		private RPCClientListener listener;
		private volatile boolean connected = false;
		private Timer timer = new Timer();

		public Connection(ConnectionId remoteId) throws IOException {
			this.remoteId = remoteId;
			this.rpcTimeout = remoteId.getRpcTimeout();
			this.maxIdleTime = remoteId.getMaxIdleTime();
			this.doPing = remoteId.getDoPing();
			this.host = remoteId.getHost();
			this.port = remoteId.getPort();
			if (doPing) {
				// construct a RPC header with the callId as the ping callId
				ByteArrayOutputStream ping = new ByteArrayOutputStream();
				RPCRequestHeader header = MsgUtil.makeRPCRequestHeader(
						MsgType.PING, RpcKind.RPC_MSGPACK,
						Constants.PING_CALL_ID, clientId, conf.getLong(
								ConfigType.IPC_PROC_TIMEOUT,
								ConfigurationKeys.IPC_PROC_TIMEOUT), null);
				MsgPackUtils.writeObject(ping, header);
				Header h = new Header(ping.size());
				Body body = new Body(ping.toByteArray());
				this.pingRequest = new Packet(h, body);
				ping.close();
			}
			this.pingInterval = remoteId.getPingInterval();
			if (LOG.isDebugEnabled()) {
				LOG.debug("The ping interval is " + this.pingInterval + " ms.");
			}
			timer.schedule(new PingTask(), 5*1000, 1 * 1000);
		}

		private class PingTask extends TimerTask {
			@Override
			public void run() {
				if ((Time.now() - lastActivity.get()) < ConfigurationKeys.IPC_PING_INTERVAL_DEFAULT) {
					if(connected){
						sendPing();
					}else{
						try {
							setupConnection();
						} catch (Exception e) {
							e.printStackTrace();
							LOG.error("try to connect server error[server="+host+":"+port+"],try agine!!!", e);
						}
					}
				}
			}
		}

		/** Update lastActivity with the current time. */
		private void touch() {
			lastActivity.set(Time.now());
		}

		/**
		 * Add a call to this connection's call queue and notify a listener;
		 * synchronized. Returns false if called during shutdown.
		 * 
		 * @param call
		 *            to add
		 * @return true if the call was added.
		 */
		private synchronized boolean addCall(Call call) {
			if (shouldCloseConnection.get())
				return false;
			calls.put(call.id, call);
			notify();
			return true;
		}

		private synchronized void setupConnection() throws Exception {
			if (!connected) {
				client = new IONettyClient();
				this.listener = new RPCClientListener(this);
				client.registerListener(listener);
				connected = client.connect(host, port, conf);
			}
		}

		/**
		 * Connect to the server and set up the I/O streams. It then sends a
		 * header to the server and starts the connection thread that waits for
		 * responses.
		 */
		private synchronized void setupIOstreams() throws Exception {
			if (shouldCloseConnection.get()) {
				return;
			}
			if (isConnected())
				return;
			try {
				while (true) {
					setupConnection();
					touch();
					return;
				}
			} catch (Exception t) {
				if (t instanceof IOException) {
					markClosed((IOException) t);
				} else {
					markClosed(new IOException("Couldn't set up IO streams", t));
				}
				close();
				throw t;
			}
		}

		private void closeConnection() {
			this.client.disconnect();
			this.connected = false;
		}

		public boolean isConnected() {
			return connected;
		}

		public void setConnected(boolean connected) {
			this.connected = connected;
		}

		public IConnection getConn() {
			return conn;
		}

		public void setConn(IConnection conn) {
			this.conn = conn;
		}
		
		public void sendPing(){
			if (shouldCloseConnection.get()) {
				return;
			}
			final DataOutputBuffer d = new DataOutputBuffer();
			RPCRequestHeader header = MsgUtil.makeRPCRequestHeader(
					MsgType.PING, RpcKind.RPC_MSGPACK, Constants.PING_CALL_ID, clientId, conf
							.getLong(ConfigType.IPC_PROC_TIMEOUT,
									ConfigurationKeys.IPC_PROC_TIMEOUT),
					"");
			try {
				synchronized (sendRpcRequestLock) {
					Packet packet = new Packet();
					MsgPackUtils.writeObject(d, header);
					final byte[] data = d.getData();
					final int totalLength = d.getLength();
					packet.getHeader().setLength(totalLength);
					packet.getBody().setContent(data);
					Connection.this.conn.send(packet);
				}
			} catch (IOException e1) {
				e1.printStackTrace();
			}finally{
				IOUtils.closeStream(d);
			}
		}

		/**
		 * Initiates a rpc call by sending the rpc request to the remote server.
		 * Note: this is not called from the Connection thread, but by other
		 * threads.
		 * 
		 * @param call
		 *            - the rpc request
		 */
		public void sendRpcRequest(final Call call)
				throws InterruptedException, IOException {
			if (shouldCloseConnection.get()) {
				return;
			}
			final DataOutputBuffer d = new DataOutputBuffer();
			RPCRequestHeader header = MsgUtil.makeRPCRequestHeader(
					MsgType.MESSAGE, call.rpcKind, call.id, clientId, conf
							.getLong(ConfigType.IPC_PROC_TIMEOUT,
									ConfigurationKeys.IPC_PROC_TIMEOUT),
					call.protoName);
			MsgPackUtils.writeObject(d, header);
			call.rpcRequest.write(d);
			synchronized (sendRpcRequestLock) {
				Future<?> senderFuture = sendParamsExecutor
						.submit(new Runnable() {
							@Override
							public void run() {
								Packet packet = new Packet();
								try {
									if (shouldCloseConnection.get()) {
										return;
									}
									final byte[] data = d.getData();
									final int totalLength = d.getLength();
									packet.getHeader().setLength(totalLength);
									packet.getBody().setContent(data);
									Connection.this.conn.send(packet);
								} catch (NoSuchElementException e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
								} catch (IllegalStateException e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
								} catch (Exception e) {
									e.printStackTrace();
								} finally {
									IOUtils.closeStream(d);
								}
							}
						});

				try {
					senderFuture.get();
				} catch (ExecutionException e) {
					Throwable cause = e.getCause();

					// cause should only be a RuntimeException as the Runnable
					// above
					// catches IOException
					if (cause instanceof RuntimeException) {
						throw (RuntimeException) cause;
					} else {
						throw new RuntimeException(
								"unexpected checked exception", cause);
					}
				}
			}
		}

		/*
		 * Receive a response. Because only one receiver, so no synchronization
		 * on in.
		 */
		private void receiveRpcResponse(byte[] content)
				throws NoSuchElementException, IllegalStateException, Exception {
			if (shouldCloseConnection.get()) {
				return;
			}
			touch();
			DataInputStream dis = new DataInputStream(new ByteArrayInputStream(
					content));
			try {
				RPCResponseHeader header = MsgPackUtils.readObject(dis,
						RPCResponseHeader.class);
				final String callId = header.getCallId();
				Call call = calls.get(callId);
				RPCStatus status = header.getStatus();
				if (status == RPCStatus.SUCCESS) {
					Writable value = ReflectionUtils.newInstance(valueClass,
							conf);
					value.readFields(dis); // read value
					calls.remove(callId);
					call.setRpcResponse(value);
				} else { // Rpc Request failed
					final String exceptionClassName = header.getErrorClass();
					final String errorMsg = header.getError();
					RemoteException re = new RemoteException(
							exceptionClassName, errorMsg);
					if (status == RPCStatus.ERROR) {
						calls.remove(callId);
						call.setException(re);
					} else if (status == RPCStatus.FATAL) {
						markClosed(re);
					}
				}
			} catch (IOException e) {
				markClosed(e);
			} finally {
				dis.close();
			}
		}

		private synchronized void markClosed(IOException e) {
			if (shouldCloseConnection.compareAndSet(false, true)) {
				closeException = e;
				notifyAll();
			}
		}

		private synchronized void markClosed() {
			if (shouldCloseConnection.compareAndSet(false, true)) {
				notifyAll();
			}
		}

		/** Close the connection. */
		private synchronized void close() {
			if (!shouldCloseConnection.get()) {
				LOG.error("The connection is not in the closed state");
				return;
			}
			connected=false;
			// release the resources
			// first thing to do;take the connection out of the connection list
			synchronized (connections) {
				if (connections.get(remoteId) == this) {
					connections.remove(remoteId);
				}
			}

			if (closeException == null) {
				if (!calls.isEmpty()) {
					LOG.warn("A connection is closed for no cause and calls are not empty");

					// clean up calls anyway
					closeException = new IOException(
							"Unexpected closed connection");
					cleanupCalls();
				}
			} else {
				// log the info
				// cleanup calls
				cleanupCalls();
			}
			closeConnection();
		}

		/* Cleanup all calls and mark them as done */
		private void cleanupCalls() {
			Iterator<Entry<String, Call>> itor = calls.entrySet().iterator();
			while (itor.hasNext()) {
				Call c = itor.next().getValue();
				itor.remove();
				c.setException(closeException); // local exception
			}
		}
	}

	/**
	 * Construct an IPC client whose values are of the given {@link Writable}
	 * class.
	 */
	public Client(Class<? extends Writable> valueClass, Configuration conf,
			String clientId) {
		this.valueClass = valueClass;
		this.conf = conf;
		this.clientId = clientId;
		this.sendParamsExecutor = clientExcecutorFactory.refAndGetInstance();
	}

	/**
	 * Stop all threads related to this client. No further calls may be made
	 * using this client.
	 */
	public void stop() {
		if (LOG.isDebugEnabled()) {
			LOG.debug("Stopping client");
		}

		if (!running.compareAndSet(true, false)) {
			return;
		}

		// wake up all connections
		synchronized (connections) {
			for (Connection conn : connections.values()) {
				conn.client.disconnect();
				conn.setConnected(false);
			}
		}

		// wait until all connections are closed
		while (!connections.isEmpty()) {
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
			}
		}

		clientExcecutorFactory.unrefAndCleanup();
	}

	/**
	 * Same as {@link #call(RPC.RpcKind, Writable, ConnectionId)} for
	 * RPC_BUILTIN
	 */
	// public Writable call(Writable param, InetSocketAddress address)
	// throws IOException {
	// return call(RpcKind., param, address);
	//
	// }

	/**
	 * Make a call, passing <code>param</code>, to the IPC server running at
	 * <code>address</code>, returning the value. Throws exceptions if there are
	 * network problems or if the remote code threw an exception.
	 * 
	 * @deprecated Use {@link #call(RPC.RpcKind, Writable, ConnectionId)}
	 *             instead
	 */
	// @Deprecated
	// public Writable call(RPC.RpcKind rpcKind, Writable param,
	// InetSocketAddress address) throws IOException {
	// return call(rpcKind, param, address, null);
	// }

	/**
	 * Make a call, passing <code>param</code>, to the IPC server running at
	 * <code>address</code> with the <code>ticket</code> credentials, returning
	 * the value. Throws exceptions if there are network problems or if the
	 * remote code threw an exception.
	 * 
	 * @deprecated Use {@link #call(RPC.RpcKind, Writable, ConnectionId)}
	 *             instead
	 */
	// @Deprecated
	// public Writable call(RPC.RpcKind rpcKind, Writable param,
	// InetSocketAddress addr, UserGroupInformation ticket)
	// throws IOException {
	// ConnectionId remoteId = ConnectionId.getConnectionId(addr, null,
	// ticket, 0, conf);
	// return call(rpcKind, param, remoteId);
	// }

	/**
	 * Make a call, passing <code>param</code>, to the IPC server running at
	 * <code>address</code> which is servicing the <code>protocol</code>
	 * protocol, with the <code>ticket</code> credentials and
	 * <code>rpcTimeout</code> as timeout, returning the value. Throws
	 * exceptions if there are network problems or if the remote code threw an
	 * exception.
	 * 
	 * @deprecated Use {@link #call(RPC.RpcKind, Writable, ConnectionId)}
	 *             instead
	 */
	// @Deprecated
	// public Writable call(RpcKind rpcKind, Writable param,
	// InetSocketAddress addr, Class<?> protocol,
	// UserGroupInformation ticket, int rpcTimeout) throws IOException {
	// ConnectionId remoteId = ConnectionId.getConnectionId(addr, protocol,
	// ticket, rpcTimeout, conf);
	// return call(rpcKind, param, remoteId);
	// }

	/**
	 * Same as
	 * {@link #call(RPC.RpcKind, Writable, InetSocketAddress, Class, UserGroupInformation, int, Configuration)}
	 * except that rpcKind is writable.
	 */
	public Writable call(Writable param, String host, int port,
			Class<?> protocol, int rpcTimeout, Configuration conf)
			throws Exception {
		ConnectionId remoteId = ConnectionId.getConnectionId(host, port,
				protocol, rpcTimeout, conf);
		return call(RpcKind.RPC_MSGPACK, param, remoteId);
	}

	/**
	 * Same as
	 * {@link #call(Writable, InetSocketAddress, Class, UserGroupInformation, int, Configuration)}
	 * except that specifying serviceClass.
	 */
	public Writable call(Writable param, String host, int port,
			Class<?> protocol, int rpcTimeout, int serviceClass,
			Configuration conf) throws Exception {
		ConnectionId remoteId = ConnectionId.getConnectionId(host, port,
				protocol, rpcTimeout, conf);
		return call(RpcKind.RPC_MSGPACK, param, remoteId);
	}

	/**
	 * Make a call, passing <code>param</code>, to the IPC server running at
	 * <code>address</code> which is servicing the <code>protocol</code>
	 * protocol, with the <code>ticket</code> credentials,
	 * <code>rpcTimeout</code> as timeout and <code>conf</code> as conf for this
	 * connection, returning the value. Throws exceptions if there are network
	 * problems or if the remote code threw an exception.
	 */
	public Writable call(RpcKind rpcKind, Writable param, String host,
			int port, Class<?> protocol, int rpcTimeout, Configuration conf)
			throws Exception {
		ConnectionId remoteId = ConnectionId.getConnectionId(host, port,
				protocol, rpcTimeout, conf);
		return call(rpcKind, param, remoteId);
	}

	/**
	 * Same as {link {@link #call(RPC.RpcKind, Writable, ConnectionId)} except
	 * the rpcKind is RPC_BUILTIN
	 */
	public Writable call(Writable param, ConnectionId remoteId)
			throws Exception {
		return call(RpcKind.RPC_MSGPACK, param, remoteId);
	}

	/**
	 * Make a call, passing <code>rpcRequest</code>, to the IPC server defined
	 * by <code>remoteId</code>, returning the rpc respond.
	 *
	 * @param rpcKind
	 * @param rpcRequest
	 *            - contains serialized method and method parameters
	 * @param remoteId
	 *            - the target rpc server
	 * @returns the rpc response Throws exceptions if there are network problems
	 *          or if the remote code threw an exception.
	 */
	// public Writable call(RpcKind rpcKind, Writable rpcRequest,
	// ConnectionId remoteId) throws IOException {
	// return call(rpcKind, rpcRequest, remoteId);
	// }

	/**
	 * Make a call, passing <code>rpcRequest</code>, to the IPC server defined
	 * by <code>remoteId</code>, returning the rpc respond.
	 *
	 * @param rpcKind
	 * @param rpcRequest
	 *            - contains serialized method and method parameters
	 * @param remoteId
	 *            - the target rpc server
	 * @param fallbackToSimpleAuth
	 *            - set to true or false during this method to indicate if a
	 *            secure client falls back to simple auth
	 * @returns the rpc response Throws exceptions if there are network problems
	 *          or if the remote code threw an exception.
	 */
	// public Writable call(RpcKind rpcKind, Writable rpcRequest,
	// ConnectionId remoteId)
	// throws IOException {
	// return call(rpcKind, rpcRequest, remoteId, fallbackToSimpleAuth);
	// }

	/**
	 * Make a call, passing <code>rpcRequest</code>, to the IPC server defined
	 * by <code>remoteId</code>, returning the rpc response.
	 * 
	 * @param rpcKind
	 * @param rpcRequest
	 *            - contains serialized method and method parameters
	 * @param remoteId
	 *            - the target rpc server
	 * @param serviceClass
	 *            - service class for RPC
	 * @returns the rpc response Throws exceptions if there are network problems
	 *          or if the remote code threw an exception.
	 */
	// public Writable call(RpcKind rpcKind, Writable rpcRequest,
	// ConnectionId remoteId, int serviceClass) throws IOException {
	// return call(rpcKind, rpcRequest, remoteId, serviceClass, null);
	// }

	/**
	 * Make a call, passing <code>rpcRequest</code>, to the IPC server defined
	 * by <code>remoteId</code>, returning the rpc response.
	 *
	 * @param rpcKind
	 * @param rpcRequest
	 *            - contains serialized method and method parameters
	 * @param remoteId
	 *            - the target rpc server
	 * @param serviceClass
	 *            - service class for RPC
	 * @param fallbackToSimpleAuth
	 *            - set to true or false during this method to indicate if a
	 *            secure client falls back to simple auth
	 * @returns the rpc response Throws exceptions if there are network problems
	 *          or if the remote code threw an exception.
	 */
	public Writable call(RpcKind rpcKind, Writable rpcRequest,
			ConnectionId remoteId) throws Exception {
		final Call call = createCall(rpcKind, rpcRequest,
				remoteId.protocol.getName());
		Connection connection = getConnection(remoteId, call);
		try {
			connection.sendRpcRequest(call); // send the rpc request
		} catch (RejectedExecutionException e) {
			throw new IOException("connection has been closed", e);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			LOG.warn("interrupted waiting to send rpc request to server", e);
			throw new IOException(e);
		}

		boolean interrupted = false;
		synchronized (call) {
			while (!call.done) {
				try {
					call.wait(); // wait for the result
				} catch (InterruptedException ie) {
					// save the fact that we were interrupted
					interrupted = true;
				}
			}

			if (interrupted) {
				// set the interrupt flag now that we are done waiting
				Thread.currentThread().interrupt();
			}

			if (call.error != null) {
				if (call.error instanceof RemoteException) {
					call.error.fillInStackTrace();
					throw call.error;
				} else { // local exception
					InetSocketAddress address = connection.client
							.getConnection().getRemoteAddress();
					throw NetUtils.wrapException(address.getHostName(),
							address.getPort(), NetUtils.getHostname(), 0,
							call.error);
				}
			} else {
				return call.getRpcResponse();
			}
		}
	}

	// for unit testing only
	Set<ConnectionId> getConnectionIds() {
		synchronized (connections) {
			return connections.keySet();
		}
	}

	/**
	 * Get a connection from the pool, or create a new one and add it to the
	 * pool. Connections to a given ConnectionId are reused.
	 */
	private Connection getConnection(ConnectionId remoteId, Call call)
			throws Exception {
		if (!running.get()) {
			// the client is stopped
			throw new IOException("The client is stopped");
		}
		Connection connection;
		/*
		 * we could avoid this allocation for each RPC by having a connectionsId
		 * object and with set() method. We need to manage the refs for keys in
		 * HashMap properly. For now its ok.
		 */
		do {
			synchronized (connections) {
				connection = connections.get(remoteId);
				if (connection == null) {
					connection = new Connection(remoteId);
					connections.put(remoteId, connection);
				}
			}
		} while (!connection.addCall(call));

		// we don't invoke the method below inside "synchronized (connections)"
		// block above. The reason for that is if the server happens to be slow,
		// it will take longer to establish a connection and that will slow the
		// entire system down.
		connection.setupIOstreams();
		return connection;
	}

	/**
	 * This class holds the address and the user ticket. The client connections
	 * to servers are uniquely identified by <remoteAddress, protocol, ticket>
	 */
	public static class ConnectionId {
		String host;
		int port;
		final Class<?> protocol;
		private static final int PRIME = 16777619;
		private final int rpcTimeout;
		private final int maxIdleTime; // connections will be culled if it was
										// idle for
		private final boolean tcpNoDelay; // if T then disable Nagle's Algorithm
		private final boolean doPing; // do we need to send ping message
		private final int pingInterval; // how often sends ping to the server in
										// msecs
		private final Configuration conf; // used to get the expected kerberos
											// principal name

		ConnectionId(String host, int port, Class<?> protocol, int rpcTimeout,
				Configuration conf) {
			this.protocol = protocol;
			this.host = host;
			this.port = port;
			this.rpcTimeout = rpcTimeout;
			this.maxIdleTime = conf.getInt(ConfigType.CLIENT_IDLE_TIME,
					ConfigurationKeys.CLIENT_IDLE_TIME);
			this.tcpNoDelay = conf.getBoolean(ConfigType.TCP_NODELY,
					ConfigurationKeys.IPC_CLIENT_TCPNODELAY_DEFAULT);
			this.doPing = conf.getBoolean(ConfigType.DO_PING,
					ConfigurationKeys.CLIENT_DO_PING);
			this.pingInterval = (doPing ? Client.getPingInterval(conf) : 0);
			this.conf = conf;
		}

		public String getHost() {
			return host;
		}

		public int getPort() {
			return port;
		}

		Class<?> getProtocol() {
			return protocol;
		}

		private int getRpcTimeout() {
			return rpcTimeout;
		}

		int getMaxIdleTime() {
			return maxIdleTime;
		}

		boolean getTcpNoDelay() {
			return tcpNoDelay;
		}

		boolean getDoPing() {
			return doPing;
		}

		int getPingInterval() {
			return pingInterval;
		}

		// static ConnectionId getConnectionId(InetSocketAddress addr,
		// Class<?> protocol, int rpcTimeout, Configuration conf)
		// throws IOException {
		// return getConnectionId(addr, protocol, rpcTimeout, null, conf);
		// }

		/**
		 * Returns a ConnectionId object.
		 * 
		 * @param addr
		 *            Remote address for the connection.
		 * @param protocol
		 *            Protocol for RPC.
		 * @param ticket
		 *            UGI
		 * @param rpcTimeout
		 *            timeout
		 * @param conf
		 *            Configuration object
		 * @return A ConnectionId instance
		 * @throws IOException
		 */
		static ConnectionId getConnectionId(String host, int port,
				Class<?> protocol, int rpcTimeout, Configuration conf)
				throws IOException {
			return new ConnectionId(host, port, protocol, rpcTimeout, conf);
		}

		static boolean isEqual(Object a, Object b) {
			return a == null ? b == null : a.equals(b);
		}

		@Override
		public boolean equals(Object obj) {
			if (obj == this) {
				return true;
			}
			if (obj instanceof ConnectionId) {
				ConnectionId that = (ConnectionId) obj;
				return isEqual(this.host, that.host) && this.port == that.port;
			}
			return false;
		}

		@Override
		public int hashCode() {
			int result = protocol.hashCode();
			result = PRIME * result + port + host.hashCode();
			result = PRIME * result + (doPing ? 1231 : 1237);
			result = PRIME * result + maxIdleTime;
			result = PRIME * result + pingInterval;
			result = PRIME * result + rpcTimeout;
			result = PRIME * result + (tcpNoDelay ? 1231 : 1237);
			return result;
		}

		@Override
		public String toString() {
			return this.host + ":" + this.port;
		}
	}

	/**
	 * Returns the next valid sequential call ID by incrementing an atomic
	 * counter and masking off the sign bit. Valid call IDs are non-negative
	 * integers in the range [ 0, 2^31 - 1 ]. Negative numbers are reserved for
	 * special purposes. The values can overflow back to 0 and be reused. Note
	 * that prior versions of the client did not mask off the sign bit, so a
	 * server may still see a negative call ID if it receives connections from
	 * an old client.
	 * 
	 * @return next call ID
	 */
	public static String nextCallId() {
		return Util.createCallId();
	}

	public class RPCClientListener implements IPacketListener {

		private Connection connection;

		public RPCClientListener(Connection conn) {
			this.connection = conn;
		}

		@Override
		public void onConnect(IConnection conn) {
			connection.setConn(conn);
		}

		@Override
		public void onReceivePacket(PacketWrapper wrapper) throws Exception {
			final Packet packet = wrapper.getPacket();
			final byte[] body = packet.getBody().getContent();
			connection.receiveRpcResponse(body);
		}

		@Override
		public void onClose(IConnection conn) {
			this.connection.markClosed();
			this.connection.close();
		}

	}
}
