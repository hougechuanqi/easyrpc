/**
 * 
 */
package com.immomo.rpc.io;

import com.immomo.rpc.configuration.Configuration;

/**
 * 
 *
 */
public interface IIOServer {

	public boolean start(Configuration config);

	public boolean shutdown();

	public boolean stop();

	public IIOServer registerListener(IPacketListener listener);
	
	public IPacketListener getListener();

}
