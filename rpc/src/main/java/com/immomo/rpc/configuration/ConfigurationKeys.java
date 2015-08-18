/**
 * 
 */
package com.immomo.rpc.configuration;

/**
 * 
 *
 */
public class ConfigurationKeys {

	public static final int IPC_CLIENT_IDLETHRESHOLD_DEFAULT = 4000;

	public static final int IPC_CLIENT_CONNECTION_MAXIDLETIME_DEFAULT = 10000; // 10s

	public static final int IPC_CLIENT_CONNECTION_IDLESCANINTERVAL_DEFAULT = 10000;

	public static final int IPC_CLIENT_KILL_MAX_DEFAULT = 10;

	public static final int IPC_SERVER_HANDLER_QUEUE_SIZE_DEFAULT = 100;

	public static final int IPC_SERVER_RPC_MAX_RESPONSE_SIZE_DEFAULT = 1024 * 1024;

	public static final String IPC_CALLQUEUE_NAMESPACE = "ipc";
	public static final String IPC_CALLQUEUE_IMPL_KEY = "callqueue.impl";
	public static final boolean IPC_CLIENT_TCPNODELAY_DEFAULT = true;
	public static final int IPC_PING_INTERVAL_DEFAULT = 5*1000;
	public static final int IPC_CALL_TIMEOUT = 1000;
	public static final int IPC_CONNECTION_TIMEOUT = 1000;
	public static final int CLIENT_IDLE_TIME=1000;
	public static final boolean CLIENT_DO_PING=true;
	public static final int IPC_PROC_TIMEOUT=1000;
	public static final int MAX_RETRY_TIMES_ON_TIMEOUT=3;
	
}
