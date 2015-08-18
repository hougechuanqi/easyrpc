/**
 * 
 */
package com.immomo.rpc.ipc;

import java.util.HashSet;

/**
 * @author shuaipenghou 2015年6月17日上午9:51:20
 *
 */
public class ProtocolProxy<T> {
	private Class<T> protocol;
	private T proxy;

	/**
	 * Constructor
	 * 
	 * @param protocol
	 *            protocol class
	 * @param proxy
	 *            its proxy
	 * @param supportServerMethodCheck
	 *            If false proxy will never fetch server methods and
	 *            isMethodSupported will always return true. If true, server
	 *            methods will be fetched for the first call to
	 *            isMethodSupported.
	 */
	public ProtocolProxy(Class<T> protocol, T proxy) {
		this.protocol = protocol;
		this.proxy = proxy;
	}

//	private void fetchServerMethods(Method method) throws IOException {
//		long clientVersion;
//		int clientMethodsHash = ProtocolSignature.getFingerprint(method
//				.getDeclaringClass().getMethods());
//		ProtocolSignature serverInfo = ((VersionedProtocol) proxy)
//				.getProtocolSignature(RPC.getProtocolName(protocol),
//						clientVersion, clientMethodsHash);
//		long serverVersion = serverInfo.getVersion();
//		if (serverVersion != clientVersion) {
//			throw new RPC.VersionMismatch(protocol.getName(), clientVersion,
//					serverVersion);
//		}
//		int[] serverMethodsCodes = serverInfo.getMethods();
//		if (serverMethodsCodes != null) {
//			serverMethods = new HashSet<Integer>(serverMethodsCodes.length);
//			for (int m : serverMethodsCodes) {
//				this.serverMethods.add(Integer.valueOf(m));
//			}
//		}
//		serverMethodsFetched = true;
//	}

	/*
	 * Get the proxy
	 */
	public T getProxy() {
		return proxy;
	}

	/**
	 * Check if a method is supported by the server or not
	 * 
	 * @param methodName
	 *            a method's name in String format
	 * @param parameterTypes
	 *            a method's parameter types
	 * @return true if the method is supported by the server
	 */
//	public synchronized boolean isMethodSupported(String methodName,
//			Class<?>... parameterTypes) throws IOException {
//		if (!supportServerMethodCheck) {
//			return true;
//		}
//		Method method;
//		try {
//			method = protocol.getDeclaredMethod(methodName, parameterTypes);
//		} catch (SecurityException e) {
//			throw new IOException(e);
//		} catch (NoSuchMethodException e) {
//			throw new IOException(e);
//		}
//		if (!serverMethodsFetched) {
//			fetchServerMethods(method);
//		}
//		if (serverMethods == null) { // client & server have the same protocol
//			return true;
//		}
//		return serverMethods.contains(Integer.valueOf(ProtocolSignature
//				.getFingerprint(method)));
//	}
}
