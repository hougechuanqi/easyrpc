/**
 * 
 */
package com.immomo.rpc.io.pool;

import java.util.NoSuchElementException;

import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPool;

import com.immomo.rpc.io.PacketWrapper;

/**
 * 
 *
 */
public class PacketWrapperPool {

	private final static ObjectPool<PacketWrapper> pool = new GenericObjectPool<PacketWrapper>(
			new PacketWrapperFactory());

	public static PacketWrapper borrowObject() throws NoSuchElementException,
			IllegalStateException, Exception {
		return pool.borrowObject();
	}

	public static void returnObject(PacketWrapper packet) throws Exception {
		pool.returnObject(packet);
	}

}
