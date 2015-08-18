/**
 * 
 */
package com.immomo.rpc.configuration;

import java.lang.ref.WeakReference;
import java.util.Collections;
import java.util.Map;
import java.util.WeakHashMap;

import com.google.common.collect.Maps;

/**
 * 
 *
 */
public class Configuration {

	private Map<ConfigType, Object> configs = Maps.newEnumMap(ConfigType.class);

	private static final Map<ClassLoader, Map<String, WeakReference<Class<?>>>> CACHE_CLASSES = new WeakHashMap<ClassLoader, Map<String, WeakReference<Class<?>>>>();

	private ClassLoader classLoader;
	{
		classLoader = Thread.currentThread().getContextClassLoader();
		if (classLoader == null) {
			classLoader = Configuration.class.getClassLoader();
		}
	}

	@SuppressWarnings("unchecked")
	public <T> T getConfig(ConfigType type) {
		return ((T) configs.get(type));
	}

	public void addConfig(ConfigType type, Object obj) {
		configs.put(type, obj);
	}

	public void addConfigDefault(ConfigType type, Object obj, Object defaultO) {
		if (obj != null) {
			configs.put(type, obj);
		} else {
			configs.put(type, defaultO);
		}
	}

	public int getInt(ConfigType type, int de) {
		if (configs.containsKey(ConfigType.CONNECTION_TIMEOUT))
			return (Integer) configs.get(ConfigType.CONNECTION_TIMEOUT);
		return de;
	}
	
	public long getLong(ConfigType type, long de) {
		if (configs.containsKey(ConfigType.IPC_PROC_TIMEOUT))
			return (Integer) configs.get(ConfigType.IPC_PROC_TIMEOUT);
		return de;
	}

	public boolean getBoolean(ConfigType type, boolean de) {
		if (configs.containsKey(ConfigType.TCP_NODELY))
			return (Boolean) configs.get(ConfigType.TCP_NODELY);
		return de;
	}

	public Object getConfig(ConfigType type, Object defaultO) {
		if (configs.containsKey(type))
			return configs.get(type);
		return defaultO;
	}

	public Class<?> getClassByName(String name) throws ClassNotFoundException {
		Class<?> ret = getClassByNameOrNull(name);
		if (ret == null) {
			throw new ClassNotFoundException("Class " + name + " not found");
		}
		return ret;
	}

	public Class<?> getClassByNameOrNull(String name) {
		Map<String, WeakReference<Class<?>>> map;

		synchronized (CACHE_CLASSES) {
			map = CACHE_CLASSES.get(classLoader);
			if (map == null) {
				map = Collections
						.synchronizedMap(new WeakHashMap<String, WeakReference<Class<?>>>());
				CACHE_CLASSES.put(classLoader, map);
			}
		}

		Class<?> clazz = null;
		WeakReference<Class<?>> ref = map.get(name);
		if (ref != null) {
			clazz = ref.get();
		}

		if (clazz == null) {
			try {
				clazz = Class.forName(name, true, classLoader);
			} catch (ClassNotFoundException e) {
				// Leave a marker that the class isn't found
				return null;
			}
			// two putters can race here, but they'll put the same class
			map.put(name, new WeakReference<Class<?>>(clazz));
			return clazz;
		} else {
			// cache hit
			return clazz;
		}
	}

}
