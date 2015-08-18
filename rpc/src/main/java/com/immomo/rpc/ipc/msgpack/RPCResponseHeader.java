/**
 * 
 */
package com.immomo.rpc.ipc.msgpack;

import org.msgpack.annotation.MessagePackBeans;


/**
 * @author shuaipenghou 2015年6月2日下午5:09:37
 */
@MessagePackBeans
public class RPCResponseHeader {
	
	private String clientId;

	private String callId;

	private RPCStatus status;

	private int errorCode;

	private String errorClass;

	private String error;

	public String getCallId() {
		return callId;
	}

	public void setCallId(String callId) {
		this.callId = callId;
	}

	public RPCStatus getStatus() {
		return status;
	}

	public void setStatus(RPCStatus status) {
		this.status = status;
	}

	public String getErrorClass() {
		return errorClass;
	}

	public void setErrorClass(String errorClass) {
		this.errorClass = errorClass;
	}

	public String getError() {
		return error;
	}

	public void setError(String error) {
		this.error = error;
	}

	public int getErrorCode() {
		return errorCode;
	}

	public void setErrorCode(int errorCode) {
		this.errorCode = errorCode;
	}

	public String getClientId() {
		return clientId;
	}

	public void setClientId(String clientId) {
		this.clientId = clientId;
	}
	
	

}
