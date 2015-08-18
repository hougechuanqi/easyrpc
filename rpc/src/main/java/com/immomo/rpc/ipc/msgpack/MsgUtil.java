/**
 * 
 */
package com.immomo.rpc.ipc.msgpack;

import com.immomo.rpc.ipc.RpcKind;

/**
 * @author shuaipenghou 2015年6月16日下午5:08:17
 *
 */
public class MsgUtil {

	public static RPCRequestHeader makeRPCRequestHeader(final MsgType msgType,
			final RpcKind rpcKind, final String callId, final String clienId,
			final long timeout,String protoName) {
		RPCRequestHeader header = new RPCRequestHeader();
		header.setCallId(callId);
		header.setRpckind(rpcKind);
		header.setTimeout(timeout);
		header.setMsgType(msgType);
		header.setProtoName(protoName);
		return header;
	}
}
