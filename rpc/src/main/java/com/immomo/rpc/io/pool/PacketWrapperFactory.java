/**
 * 
 */
package com.immomo.rpc.io.pool;

import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;

import com.immomo.rpc.io.PacketWrapper;

/**
 * 
 *
 */
public class PacketWrapperFactory extends
		BasePooledObjectFactory<PacketWrapper> {

	@Override
	public PacketWrapper create() throws Exception {
		return new PacketWrapper();
	}

	@Override
	public PooledObject<PacketWrapper> wrap(PacketWrapper obj) {
		return new DefaultPooledObject<PacketWrapper>(obj);
	}

	@Override
	public void passivateObject(PooledObject<PacketWrapper> p) throws Exception {
		p.getObject().clear();
	}
}
