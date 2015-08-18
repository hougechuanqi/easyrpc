/**
 * 
 */
package com.immomo.rpc.io;

/**
 * 
 *
 */
public abstract class AbstractIOServer implements IIOServer {

	protected IPacketListener listener;

	@Override
	public IIOServer registerListener(IPacketListener listener) {
		this.listener = listener;
		return this;
	}

	@Override
	public IPacketListener getListener() {
		return listener;
	}

}
