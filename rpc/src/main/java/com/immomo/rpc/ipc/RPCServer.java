/**
 * 
 */
package com.immomo.rpc.ipc;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.immomo.rpc.configuration.ConfigType;
import com.immomo.rpc.configuration.Configuration;
import com.immomo.rpc.configuration.ConfigurationKeys;
import com.immomo.rpc.io.IIOServer;
import com.immomo.rpc.io.IPacketListener;
import com.immomo.rpc.io.PacketWrapper;
import com.immomo.rpc.io.Writable;
import com.immomo.rpc.io.connection.IConnection;
import com.immomo.rpc.io.server.netty.server.NettyIOServer;
import com.immomo.rpc.ipc.RPC.RpcInvoker;
import com.immomo.rpc.ipc.msgpack.RPCRequestHeader;
import com.immomo.rpc.ipc.msgpack.RPCResponseHeader;
import com.immomo.rpc.ipc.msgpack.RPCStatus;
import com.immomo.rpc.statistics.ServerStatistics;
import com.immomo.rpc.statistics.StatisticsMgr;
import com.immomo.rpc.util.MsgPackUtils;
import com.immomo.rpc.util.ReflectionUtils;
import com.immomo.rpc.util.StringUtils;
import com.immomo.rpc.util.Time;

/**
 * 
 *
 */
public abstract class RPCServer { 

	public static final Log LOG = LogFactory.getLog(RPCServer.class);

	private String bindAddress;
	private int port;
	private int maxQueueSize;
	private int maxRespSize;
	volatile private boolean running = true;
	private ConnectionManager connectionManager;
	private Handler[] handlers = null;
	private Configuration conf;
	// private Class<? extends Writable> rpcRequestClass;
	private CallQueueManager<Call> callQueue;
	private IPacketListener listener;
	private int handlerCount = 1;
	static int INITIAL_RESP_BUF_SIZE = 1024 * 1024;
	private static final ThreadLocal<RPCServer> SERVER = new ThreadLocal<RPCServer>();
	private static final ThreadLocal<Call> CurCall = new ThreadLocal<Call>();
	private ExceptionsHandler exceptionsHandler = new ExceptionsHandler();
	private AtomicLong callQueueSize = new AtomicLong();
	private IIOServer ioServer;

	// private ServerDaemon serverDaemon;

	public RPCServer(String bindAddress, int port, Configuration conf,
			String serverName, int handlerCount) {
		this.bindAddress = bindAddress;
		this.conf = conf;
		this.port = port;
		this.handlerCount = handlerCount;
		this.maxRespSize = ConfigurationKeys.IPC_SERVER_HANDLER_QUEUE_SIZE_DEFAULT;
		this.maxRespSize = ConfigurationKeys.IPC_SERVER_RPC_MAX_RESPONSE_SIZE_DEFAULT;
		final String prefix = getQueueClassPrefix();
		this.callQueue = new CallQueueManager<Call>(
				getQueueClass(prefix, conf), maxQueueSize, prefix, conf);
		connectionManager = new ConnectionManager();
		listener = new RPCListener();
		// serverDaemon = new ServerDaemon(conf);
	}

	static class ExceptionsHandler {
		private volatile Set<String> terseExceptions = new HashSet<String>();

		/**
		 * Add exception class so server won't log its stack trace. Modifying
		 * the terseException through this method is thread safe.
		 *
		 * @param exceptionClass
		 *            exception classes
		 */
		void addTerseExceptions(Class<?>... exceptionClass) {

			// Make a copy of terseException for performing modification
			final HashSet<String> newSet = new HashSet<String>(terseExceptions);

			// Add all class names into the HashSet
			for (Class<?> name : exceptionClass) {
				newSet.add(name.toString());
			}
			// Replace terseException set
			terseExceptions = Collections.unmodifiableSet(newSet);
		}

		boolean isTerse(Class<?> t) {
			return terseExceptions.contains(t.toString());
		}
	}

	public static class Call {
		private final String callId; // the client's call id
		private final Writable rpcRequest; // Serialized Rpc request from client
		private final IConnection connection; // connection to client
		private long timestamp; // time received when response is null
								// time served when response is not null
		private ByteBuffer rpcResponse; // the response for this call
		private final RpcKind rpcKind;
		private final String clientId;
		private final String protocolName;

		public Call(String id, Writable param, IConnection connection,
				String protocolName) {
			this(id, param, connection, RpcKind.RPC_MSGPACK, connection
					.getSessionId(), protocolName);
		}

		public Call(String id, Writable param, IConnection connection,
				RpcKind kind, String clientId, String protocolName) {
			this.callId = id;
			this.rpcRequest = param;
			this.connection = connection;
			this.timestamp = Time.now();
			this.rpcResponse = null;
			this.rpcKind = kind;
			this.clientId = clientId;
			this.protocolName = protocolName;
		}

		@Override
		public String toString() {
			return rpcRequest + " from " + connection + " Call#" + callId
					+ " Retry#";
		}

		public ByteBuffer getRpcResponse() {
			return rpcResponse;
		}

		public String getCallId() {
			return callId;
		}

		public Writable getRpcRequest() {
			return rpcRequest;
		}

		public IConnection getConnection() {
			return connection;
		}

		public long getTimestamp() {
			return timestamp;
		}

		public RpcKind getRpcKind() {
			return rpcKind;
		}

		public String getClientId() {
			return clientId;
		}

		public String getProtocolName() {
			return protocolName;
		}

		public void setResponse(ByteBuffer response) {
			this.rpcResponse = response;
		}
	}

	// private class ServerDaemon extends Thread {
	// Configuration config;
	//
	// public ServerDaemon(Configuration config) {
	// this.config = config;
	// }
	//
	// @Override
	// public void run() {
	// ioServer.start(config);
	// }
	//
	// }

	private class Handler extends Thread {
		public Handler(int instanceNumber) {
			this.setDaemon(true);
			this.setName("IPC Server handler " + instanceNumber + " on " + port);
		}

		@Override
		public void run() {
			SERVER.set(RPCServer.this);
			ByteArrayOutputStream buf = new ByteArrayOutputStream(
					INITIAL_RESP_BUF_SIZE);
			while (running) {
				try {
					Call call = callQueue.take();

					CurCall.set(call);
					if (!call.connection.isAlive()) {
						continue;
					}
					String errorClass = null;
					String error = null;
					RPCStatus status = RPCStatus.SUCCESS;
					Writable value = null;
					try {
						value = call(call.rpcKind, call.protocolName,
								call.rpcRequest, call.timestamp);
					} catch (Throwable e) {
						if(StatisticsMgr.isInitialized){
						     StatisticsMgr.getInstance().addErrorNum();
						}
						if (e instanceof UndeclaredThrowableException) {
							e = e.getCause();
						}
						// String logMsg = Thread.currentThread().getName()
						// + ", call " + call;
						if (exceptionsHandler.isTerse(e.getClass())) {
							// Don't log the whole stack trace. Way too noisy!
						} else if (e instanceof RuntimeException
								|| e instanceof Error) {
						} else {
						}
						status = RPCStatus.ERROR;
						errorClass = e.getClass().getName();
						error = StringUtils.stringifyException(e);
						// Remove redundant error class name from the beginning
						// of the stack trace
						String exceptionHdr = errorClass + ": ";
						if (error.startsWith(exceptionHdr)) {
							error = error.substring(exceptionHdr.length());
						}
					}
					CurCall.set(null);
					synchronized (call.connection) {
						setupResponse(buf, call, status, value, errorClass,
								error);
						if (buf.size() > maxRespSize) {
							LOG.warn("Large response size " + buf.size()
									+ " for call " + call.toString());
							buf = new ByteArrayOutputStream(
									INITIAL_RESP_BUF_SIZE);
						}
						call.connection.doResponse(call);
						
						RPCServer.this.callQueueSize.decrementAndGet();
						if(StatisticsMgr.isInitialized){
						    StatisticsMgr.getInstance().addProcSize();
						}
					}
				} catch (InterruptedException e1) {
					LOG.error("Invocation Call Exception", e1);
					LOG.debug(Thread.currentThread().getName() + ": exiting");
				} catch (IOException e) {
					LOG.error("Invocation Call Exception", e);
				} finally {

				}
			}
		}

	}

	private void setupResponse(ByteArrayOutputStream responseBuf, Call call,
			RPCStatus status, Writable rv, String errorClass, String error)
			throws IOException {
		responseBuf.reset();
		DataOutputStream out = new DataOutputStream(responseBuf);
		RPCResponseHeader header = new RPCResponseHeader();
		header.setCallId(call.callId);
		header.setStatus(status);
		if (status == RPCStatus.SUCCESS) {
			MsgPackUtils.writeObject(out, header);
			rv.write(out);
		} else { // Rpc Failure
			header.setErrorClass(errorClass);
			header.setError(error);
			MsgPackUtils.writeObject(out, header);
		}
		call.setResponse(ByteBuffer.wrap(responseBuf.toByteArray()));
	}

	public abstract Writable call(RpcKind rpcKind, String protocol,
			Writable param, long receiveTime) throws Exception;

	public class RPCListener implements IPacketListener {

		@Override
		public void onConnect(IConnection conn) {
			connectionManager.add(conn);
		}

		@Override
		public void onReceivePacket(PacketWrapper wrapper) throws Exception {
			final byte[] content = wrapper.getPacket().getBody().getContent();
			DataInputStream dis = new DataInputStream(new ByteArrayInputStream(
					content));
			processRpcRequest(dis, wrapper.getConn());
		}

		@Override
		public void onClose(IConnection conn) {
			connectionManager.remove(conn);
		}

		private void processRpcRequest(DataInputStream dis, IConnection conn)
				throws Exception {
			try {
				RPCRequestHeader header = MsgPackUtils.readObject(dis,
						RPCRequestHeader.class);
				switch (header.getMsgType()) {
				case MESSAGE:
					Class<? extends Writable> rpcRequestClass = getRpcRequestWrapper(header
							.getRpckind());
					if (rpcRequestClass == null) {
						final String err = "Unknown rpc kind in rpc header"
								+ header.getRpckind();
						throw new Exception(err);
					}
					Writable rpcRequest;
					try { // Read the rpc request
						rpcRequest = ReflectionUtils.newInstance(
								rpcRequestClass, conf);
						rpcRequest.readFields(dis);
					} catch (Throwable t) { // includes runtime exception from
						LOG.warn("Unable to read call parameters for client "
								+ conn.getRemoteAddress()
								+ "on connection protocol " + " for rpcKind "
								+ header.getRpckind(), t);
						String err = "IPC server unable to read call parameters: "
								+ t.getMessage();
						throw new Exception(err);
					}
					Call call = new Call(header.getCallId(), rpcRequest, conn,
							header.getRpckind(), conn.getSessionId(),
							header.getProtoName());
					incCallQueueSize();
					callQueue.put(call); // queue the call; maybe blocked here
					
					if(StatisticsMgr.isInitialized){
					      StatisticsMgr.getInstance().addRecSize();
					}
					break;
				case PING:
					break;
				default:
					break;
				}

			} catch (Exception e) {
				LOG.error(ExceptionUtils.getStackTrace(e));
			} finally {
				dis.close();
			}

		}

		private void incCallQueueSize() {
			callQueueSize.incrementAndGet();
		}
	}

	static Class<? extends BlockingQueue<Call>> getQueueClass(String prefix,
			Configuration conf) {
		String name = prefix + "." + ConfigurationKeys.IPC_CALLQUEUE_IMPL_KEY;
		Class<?> queueClass = LinkedBlockingQueue.class;
		return CallQueueManager.convertQueueClass(queueClass, Call.class);
	}

	private String getQueueClassPrefix() {
		return ConfigurationKeys.IPC_CALLQUEUE_NAMESPACE + "." + port;
	}

	private class ConnectionManager {
		final private AtomicInteger count = new AtomicInteger();
		final private Set<IConnection> connections;
		final private Timer idleScanTimer;
		/***
		 * idle scan threshold
		 */
		final private int idleScanThreshold;
		final private int idleScanInterval;
		final private int maxIdleTime;
		final private int maxIdleToClose;

		ConnectionManager() {
			this.idleScanTimer = new Timer(
					"IPC Server idle connection scanner for port " + getPort(),
					true);
			this.idleScanThreshold = ConfigurationKeys.IPC_CLIENT_IDLETHRESHOLD_DEFAULT;
			this.idleScanInterval = ConfigurationKeys.IPC_CLIENT_CONNECTION_IDLESCANINTERVAL_DEFAULT;
			this.maxIdleTime = ConfigurationKeys.IPC_CLIENT_CONNECTION_MAXIDLETIME_DEFAULT;
			this.maxIdleToClose = ConfigurationKeys.IPC_CLIENT_KILL_MAX_DEFAULT;
			this.connections = Collections
					.newSetFromMap(new ConcurrentHashMap<IConnection, Boolean>(
							maxQueueSize, 0.75f, Runtime.getRuntime()
									.availableProcessors() * 2 + 1));
		}

		private boolean add(IConnection connection) {
			boolean added = connections.add(connection);
			if (added) {
				count.getAndIncrement();
			}
			return added;
		}

		private boolean remove(IConnection connection) {
			boolean removed = connections.remove(connection);
			if (removed) {
				count.getAndDecrement();
			}
			return removed;
		}

		int size() {
			return count.get();
		}

		IConnection[] toArray() {
			return connections.toArray(new IConnection[0]);
		}

		boolean close(IConnection connection) {
			boolean exists = remove(connection);
			if (exists) {
				connection.close();
			}
			return exists;
		}

		synchronized void closeIdle(boolean scanAll) {
			long minLastContact = Time.now() - maxIdleTime;
			// concurrent iterator might miss new connections added
			// during the iteration, but that's ok because they won't
			// be idle yet anyway and will be caught on next scan
			int closed = 0;
			for (IConnection connection : connections) {
				// stop if connections dropped below threshold unless scanning
				// all
				if (!scanAll && size() < idleScanThreshold) {
					break;
				}
				// stop if not scanning all and max connections are closed
				if (connection.isIdle()
						&& connection.getLastContact() < minLastContact
						&& close(connection) && !scanAll
						&& (++closed == maxIdleToClose)) {
					break;
				}
			}
		}

		void closeAll() {
			// use a copy of the connections to be absolutely sure the
			// concurrent
			// iterator doesn't miss a connection
			for (IConnection connection : toArray()) {
				close(connection);
			}
		}

		void startIdleScan() {
			scheduleIdleScanTask();
		}

		void stopIdleScan() {
			idleScanTimer.cancel();
		}

		private void scheduleIdleScanTask() {
			if (!running) {
				return;
			}
			TimerTask idleScanTask = new TimerTask() {
				@Override
				public void run() {
					if (!running) {
						return;
					}
					try {
						closeIdle(false);
					} finally {
						scheduleIdleScanTask();
					}
				}
			};
			idleScanTimer.schedule(idleScanTask, idleScanInterval);
		}

	}

	static class RpcKindMapValue {
		final Class<? extends Writable> rpcRequestWrapperClass;
		final RpcInvoker rpcInvoker;

		RpcKindMapValue(Class<? extends Writable> rpcRequestWrapperClass,
				RpcInvoker rpcInvoker) {
			this.rpcInvoker = rpcInvoker;
			this.rpcRequestWrapperClass = rpcRequestWrapperClass;
		}
	}

	public static void registerProtocolEngine(RpcKind rpcKind,
			Class<? extends Writable> rpcRequestWrapperClass,
			RpcInvoker rpcInvoker) {
		RpcKindMapValue old = rpcKindMap.put(rpcKind, new RpcKindMapValue(
				rpcRequestWrapperClass, rpcInvoker));
		if (old != null) {
			rpcKindMap.put(rpcKind, old);
			throw new IllegalArgumentException("ReRegistration of rpcKind: "
					+ rpcKind);
		}
	}

	static Map<RpcKind, RpcKindMapValue> rpcKindMap = new HashMap<RpcKind, RpcKindMapValue>(
			4);

	public Class<? extends Writable> getRpcRequestWrapper(RpcKind rpcKind) {
		RpcKindMapValue val = rpcKindMap.get(rpcKind);
		return (val == null) ? null : val.rpcRequestWrapperClass;
	}

	public synchronized void start() {
		this.conf.addConfig(ConfigType.PORT, this.port);
		ioServer = new NettyIOServer();
		ioServer.registerListener(listener);
		ioServer.start(conf);
		handlers = new Handler[handlerCount];
		for (int i = 0; i < handlerCount; i++) {
			handlers[i] = new Handler(i);
			handlers[i].start();
		}
		connectionManager.startIdleScan();
		
		//statistics thread
		if(StatisticsMgr.isInitialized){
		    StatisticsMgr.getInstance().init(this);
		    ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
		    executor.scheduleAtFixedRate(new ServerStatistics(),0, 1000, TimeUnit.MILLISECONDS);
		}
	}

	/** Stops the service. No new calls will be handled after this is called. */
	public synchronized void stop() {
		LOG.info("Stopping server on " + port);
		running = false;
		if (handlers != null) {
			for (int i = 0; i < handlerCount; i++) {
				if (handlers[i] != null) {
					handlers[i].interrupt();
				}
			}
		}
		this.ioServer.shutdown();
		connectionManager.stopIdleScan();
	}

	public int getPort() {
		return port;
	}

	public static RpcInvoker getRpcInvoker(RpcKind rpcKind) {
		RpcKindMapValue val = rpcKindMap.get(rpcKind);
		return (val == null) ? null : val.rpcInvoker;
	}

	public int getHandlerCount() {
		return handlerCount;
	}

	public int getCallQueueSize(){
		return callQueueSize.intValue();
	}
	
	public int getTcpConnectNum(){
		return connectionManager.count.get();
	}
}
