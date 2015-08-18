/**
 * 
 */
package com.immomo.rpc.io;

import com.immomo.rpc.configuration.Configuration;
import com.immomo.rpc.io.connection.IConnection;

/**
 * @author shuaipenghou 2015年6月17日下午4:53:31
 *
 */
public interface IIOClient {

	public boolean connect(String host, int port, Configuration conf)
			throws Exception;

	public boolean disconnect();

	public IIOClient registerListener(IPacketListener listener);

	public IPacketListener getListener();

	public IConnection getConnection();
	
	public void setConnection(IConnection conn);

}
