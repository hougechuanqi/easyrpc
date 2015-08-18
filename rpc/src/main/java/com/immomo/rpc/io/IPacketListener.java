/**
 * 
 */
package com.immomo.rpc.io;

import com.immomo.rpc.io.connection.IConnection;

/**
 * 
 *
 */
public interface IPacketListener {

	/***
	 * when a session is on connect
	 * 
	 * @param conn
	 */
	public void onConnect(IConnection conn);

	/***
	 * wrapper is passer object,so wrapper is object pool
	 * 
	 * @param wrapper
	 */
	public void onReceivePacket(PacketWrapper wrapper) throws Exception;

	/**
	 * when a session is on close
	 * 
	 * @param conn
	 */
	public void onClose(IConnection conn);

}
