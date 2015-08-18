/**
 * 
 */
package com.immomo.rpc.ipc.msgpack;

import org.msgpack.annotation.Message;
import org.msgpack.annotation.MessagePackBeans;

import com.immomo.rpc.ipc.RpcKind;

/**
 * @author shuaipenghou 2015年6月1日下午7:04:46
 *
 */
@MessagePackBeans
public class RPCRequestHeader {

	/**
	 * request call id
	 */
	private String callId;

	/***
	 * request rpckind
	 */
	private RpcKind rpckind;

	/***
	 * message type ping or normal message
	 */
	private MsgType msgType;

	/***
	 * request timeout
	 */
	private long timeout;

	private String protoName;

	public String getCallId() {
		return callId;
	}

	public void setCallId(String callId) {
		this.callId = callId;
	}

	public RpcKind getRpckind() {
		return rpckind;
	}

	public void setRpckind(RpcKind rpckind) {
		this.rpckind = rpckind;
	}

	public long getTimeout() {
		return timeout;
	}

	public void setTimeout(long timeout) {
		this.timeout = timeout;
	}

	public String getProtoName() {
		return protoName;
	}

	public void setProtoName(String protoName) {
		this.protoName = protoName;
	}

	public MsgType getMsgType() {
		return msgType;
	}

	public void setMsgType(MsgType msgType) {
		this.msgType = msgType;
	}

}
