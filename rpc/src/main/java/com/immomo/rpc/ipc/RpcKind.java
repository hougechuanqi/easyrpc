package com.immomo.rpc.ipc;

import org.msgpack.annotation.MessagePackOrdinalEnum;


@MessagePackOrdinalEnum
public enum RpcKind {
	RPC_MSGPACK((short) 1), // Used for built in calls by tests
	;
	final static short MAX_INDEX = RPC_MSGPACK.value; // used for
														// array
														// size
	public final short value; // TODO make it private

	RpcKind(short val) {
		this.value = val;
	}
}