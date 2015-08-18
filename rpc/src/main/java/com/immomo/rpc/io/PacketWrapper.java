/**
 * 
 */
package com.immomo.rpc.io;

import com.immomo.rpc.io.connection.IConnection;
import com.immomo.rpc.io.msg.Packet;

/**
 * 
 *
 */
public class PacketWrapper {

	private IConnection conn;

	private Packet packet;

	public IConnection getConn() {
		return conn;
	}

	public void setConn(IConnection conn) {
		this.conn = conn;
	}

	public Packet getPacket() {
		return packet;
	}

	public void setPacket(Packet packet) {
		this.packet = packet;
	}

	public void clear() {
		this.conn = null;
		this.packet = null;
	}

}
