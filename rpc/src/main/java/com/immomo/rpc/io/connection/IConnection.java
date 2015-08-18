/**
 * 
 */
package com.immomo.rpc.io.connection;

import java.net.InetSocketAddress;

import com.immomo.rpc.io.msg.Packet;
import com.immomo.rpc.ipc.RPCServer.Call;

/**
 * 
 *
 */
public interface IConnection {

	public static String CONNBINDNAME = "conn";

	public InetSocketAddress  getRemoteAddress();

	public String getSessionId();

	public boolean send(Packet packet);

	public boolean close();

	public long getLastContact();

	public boolean isIdle();

	public boolean isAlive();
	
	public void doResponse(Call call);
	

}
