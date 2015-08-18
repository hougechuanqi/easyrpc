/**
 * 
 */
package com.immomo.rpc.exception;

import java.net.SocketTimeoutException;

/**
 * 
 *
 */
public class ConnectTimeoutException extends SocketTimeoutException {

	private static final long serialVersionUID = 1L;

	public ConnectTimeoutException(String msg) {
		super(msg);
	}

}
