/**
 * 
 */
package com.immomo.rpc.ipc.msgpack;

import org.msgpack.annotation.MessagePackOrdinalEnum;


/**
 * 
 *
 */
@MessagePackOrdinalEnum
public enum RPCStatus {

	/****
	 * <code>SUCCESS = 0;</code>
	 * 
	 * <pre>
	 * RPC success
	 * </pre>
	 */
	SUCCESS(0, 0),
	/**
	 * <code>ERROR = 1;</code>
	 *
	 * <pre>
	 * RPC or error - connection left open for future calls
	 * </pre>
	 */
	ERROR(1, 1),
	/**
	 * <code>FATAL = 2;</code>
	 *
	 * <pre>
	 * Fatal error - connection closed
	 * </pre>
	 */
	FATAL(2, 2),

	;

	private final int id;
	private final int code;

	private RPCStatus(final int id, final int code) {
		this.id = id;
		this.code = code;
	}

	public int getId() {
		return id;
	}

	public int getCode() {
		return code;
	}

}
