/**
 * 
 */
package com.immomo.rpc.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Array;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.immomo.rpc.configuration.Configuration;
import com.immomo.rpc.configuration.Configured;
import com.immomo.rpc.util.MsgPackUtils;
import com.immomo.rpc.util.ReflectionUtils;

public class ObjectWritable extends Configured implements Writable {

	private Class declaredClass;
	private Object instance;
	private Configuration conf;

	public ObjectWritable() {
	}

	public ObjectWritable(Object instance) {
		set(instance);
	}

	public ObjectWritable(Class declaredClass, Object instance) {
		this.declaredClass = declaredClass;
		this.instance = instance;
	}

	/** Return the instance, or null if none. */
	public Object get() {
		return instance;
	}

	/** Return the class this is meant to be. */
	public Class getDeclaredClass() {
		return declaredClass;
	}

	/** Reset the instance. */
	public void set(Object instance) {
		this.declaredClass = instance.getClass();
		this.instance = instance;
	}

	@Override
	public String toString() {
		return "OW[class=" + declaredClass + ",value=" + instance + "]";
	}

	@Override
	public void readFields(DataInput in) throws Exception {
		readObject(in, this, this.conf);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		writeObject(out, instance, declaredClass, conf);
	}

	private static final Map<String, Class<?>> PRIMITIVE_NAMES = new HashMap<String, Class<?>>();
	static {
		PRIMITIVE_NAMES.put("boolean", Boolean.TYPE);
		PRIMITIVE_NAMES.put("byte", Byte.TYPE);
		PRIMITIVE_NAMES.put("char", Character.TYPE);
		PRIMITIVE_NAMES.put("short", Short.TYPE);
		PRIMITIVE_NAMES.put("int", Integer.TYPE);
		PRIMITIVE_NAMES.put("long", Long.TYPE);
		PRIMITIVE_NAMES.put("float", Float.TYPE);
		PRIMITIVE_NAMES.put("double", Double.TYPE);
		PRIMITIVE_NAMES.put("void", Void.TYPE);
	}

	private static class NullInstance extends Configured implements Writable {
		private Class<?> declaredClass;

		public NullInstance() {
			super();
		}

		public NullInstance(Class declaredClass, Configuration conf) {
			this.declaredClass = declaredClass;
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			String className = MsgPackUtils.readString(in);
			declaredClass = PRIMITIVE_NAMES.get(className);
			if (declaredClass == null) {
				try {
					declaredClass = getConf().getClassByName(className);
				} catch (ClassNotFoundException e) {
					throw new RuntimeException(e.toString());
				}
			}
		}

		@Override
		public void write(DataOutput out) throws IOException {
			MsgPackUtils.writeString(out, declaredClass.getName());
		}
	}

	/**
	 * Write a {@link Writable}, {@link String}, primitive type, or an array of
	 * the preceding.
	 */
	public static void writeObject(DataOutput out, Object instance,
			Class declaredClass, Configuration conf) throws IOException {
		writeObject(out, instance, declaredClass, conf, false);
	}

	/**
	 * Write a {@link Writable}, {@link String}, primitive type, or an array of
	 * the preceding.
	 * 
	 * @param allowCompactArrays
	 *            - set true for RPC and internal or intra-cluster usages. Set
	 *            false for inter-cluster, File, and other persisted output
	 *            usages, to preserve the ability to interchange files with
	 *            other clusters that may not be running the same version of
	 *            software. Sometime in ~2013 we can consider removing this
	 *            parameter and always using the compact format.
	 */
	public static void writeObject(DataOutput out, Object instance,
			Class declaredClass, Configuration conf, boolean allowCompactArrays)
			throws IOException {

		if (instance == null) { // null
			instance = new NullInstance(declaredClass, conf);
			declaredClass = Writable.class;
		}

		// Special case: must come before writing out the declaredClass.
		// If this is an eligible array of primitives,
		// wrap it in an ArrayPrimitiveWritable$Internal wrapper class.
		if (allowCompactArrays
				&& declaredClass.isArray()
				&& instance.getClass().getName()
						.equals(declaredClass.getName())
				&& instance.getClass().getComponentType().isPrimitive()) {
			instance = new ArrayPrimitiveWritable.Internal(instance);
			declaredClass = ArrayPrimitiveWritable.Internal.class;
		}

		MsgPackUtils.writeString(out, declaredClass.getName());

		if (declaredClass.isArray()) { // non-primitive or non-compact array
			int length = Array.getLength(instance);
			MsgPackUtils.writeInt(out, length);
			for (int i = 0; i < length; i++) {
				writeObject(out, Array.get(instance, i),
						declaredClass.getComponentType(), conf,
						allowCompactArrays);
			}
		} else if (declaredClass == ArrayPrimitiveWritable.Internal.class) {
			((ArrayPrimitiveWritable.Internal) instance).write(out);

		} else if (declaredClass == String.class) { // String
			MsgPackUtils.writeString(out, (String) instance);

		} else if (declaredClass.isPrimitive()) { // primitive type
			if (declaredClass == Boolean.TYPE) { // boolean
				MsgPackUtils.writeBoolean(out,
						((Boolean) instance).booleanValue());
			} else if (declaredClass == Character.TYPE) { // char
				MsgPackUtils.writeChar(out, ((Character) instance).charValue());
			} else if (declaredClass == Byte.TYPE) { // byte
				MsgPackUtils.writeByte(out, ((Byte) instance).byteValue());
			} else if (declaredClass == Short.TYPE) { // short
				MsgPackUtils.writeShort(out, ((Short) instance).shortValue());
			} else if (declaredClass == Integer.TYPE) { // int
				MsgPackUtils.writeInt(out, ((Integer) instance).intValue());
			} else if (declaredClass == Long.TYPE) { // long
				MsgPackUtils.writeLong(out, ((Long) instance).longValue());
			} else if (declaredClass == Float.TYPE) { // float
				MsgPackUtils.writeFloat(out, ((Float) instance).floatValue());
			} else if (declaredClass == Double.TYPE) { // double
				MsgPackUtils
						.writeDouble(out, ((Double) instance).doubleValue());
			} else if (declaredClass == Void.TYPE) { // void
			} else {
				throw new IllegalArgumentException("Not a primitive: "
						+ declaredClass);
			}
		} else if (declaredClass.isEnum()) { // enum
			MsgPackUtils.writeString(out, ((Enum) instance).name());
		} else if(List.class.isAssignableFrom(declaredClass)){
			MsgPackUtils.writeList(out, (List)instance);
		}else if(Map.class.isAssignableFrom(declaredClass)){
			MsgPackUtils.writeMap(out, (Map)instance);
		}
		else {
			MsgPackUtils.writeObject(out, instance);
		}
	}

	/**
	 * Read a {@link Writable}, {@link String}, primitive type, or an array of
	 * the preceding.
	 */
	public static Object readObject(DataInput in, Configuration conf)
			throws Exception {
		return readObject(in, null, conf);
	}

	/**
	 * Read a {@link Writable}, {@link String}, primitive type, or an array of
	 * the preceding.
	 * @throws  
	 */
	@SuppressWarnings("unchecked")
	public static Object readObject(DataInput in,
			ObjectWritable objectWritable, Configuration conf)
			throws Exception {
		String className = MsgPackUtils.readString(in);
		Class<?> declaredClass = PRIMITIVE_NAMES.get(className);
		if (declaredClass == null) {
			declaredClass = loadClass(conf, className);
		}
		Object instance = null;
		if (declaredClass.isPrimitive()) { // primitive types
			if (declaredClass == Boolean.TYPE) { // boolean
				instance = Boolean.valueOf(MsgPackUtils.readBoolean(in));
			} else if (declaredClass == Character.TYPE) { // char
				instance = Character.valueOf(MsgPackUtils.readChar(in));
			} else if (declaredClass == Byte.TYPE) { // byte
				instance = Byte.valueOf(MsgPackUtils.readByte(in));
			} else if (declaredClass == Short.TYPE) { // short
				instance = Short.valueOf(MsgPackUtils.readShort(in));
			} else if (declaredClass == Integer.TYPE) { // int
				instance = Integer.valueOf(MsgPackUtils.readInt(in));
			} else if (declaredClass == Long.TYPE) { // long
				instance = Long.valueOf(MsgPackUtils.readLong(in));
			} else if (declaredClass == Float.TYPE) { // float
				instance = Float.valueOf(MsgPackUtils.readFloat(in));
			} else if (declaredClass == Double.TYPE) { // double
				instance = Double.valueOf(MsgPackUtils.readDouble(in));
			} else if (declaredClass == Void.TYPE) { // void
				instance = null;
			} else {
				throw new IllegalArgumentException("Not a primitive: "
						+ declaredClass);
			}

		} else if (declaredClass.isArray()) { // array
			int length = MsgPackUtils.readInt(in);
			instance = Array.newInstance(declaredClass.getComponentType(),
					length);
			for (int i = 0; i < length; i++) {
				Array.set(instance, i, readObject(in, conf));
			}
		} else if (declaredClass == ArrayPrimitiveWritable.Internal.class) {
			ArrayPrimitiveWritable.Internal temp = new ArrayPrimitiveWritable.Internal();
			temp.readFields(in);
			instance = temp.get();
			declaredClass = instance.getClass();

		} else if (declaredClass == String.class) { // String
			instance = MsgPackUtils.readString(in);
		} else if (declaredClass.isEnum()) { // enum
			instance = Enum.valueOf((Class<? extends Enum>) declaredClass,
					MsgPackUtils.readString(in));
		} else if (Writable.class.isAssignableFrom(declaredClass)) {
			String clazzName = MsgPackUtils.readString(in);
			((Writable) ReflectionUtils.newInstance(loadClass(conf, clazzName),
					conf)).readFields(in);
		} else if(List.class.isAssignableFrom(declaredClass)){
			instance=MsgPackUtils.readList(in);
		}else if(Map.class.isAssignableFrom(declaredClass)){
			instance=MsgPackUtils.readMap(in);
		}
		else { // Writable
			instance = MsgPackUtils.readObject(in, declaredClass);
		}
		if (objectWritable != null) { // store values
			objectWritable.declaredClass = declaredClass;
			objectWritable.instance = instance;
		}
		return instance;

	}

	static Method getStaticProtobufMethod(Class<?> declaredClass,
			String method, Class<?>... args) {

		try {
			return declaredClass.getMethod(method, args);
		} catch (Exception e) {
			// This is a bug in Hadoop - protobufs should all have this static
			// method
			throw new AssertionError(
					"Protocol buffer class "
							+ declaredClass
							+ " does not have an accessible parseFrom(InputStream) method!");
		}
	}

	/**
	 * Find and load the class with given name <tt>className</tt> by first
	 * finding it in the specified <tt>conf</tt>. If the specified <tt>conf</tt>
	 * is null, try load it directly.
	 */
	public static Class<?> loadClass(Configuration conf, String className) {
		Class<?> declaredClass = null;
		try {
			if (conf != null)
				declaredClass = conf.getClassByName(className);
			else
				declaredClass = Class.forName(className);
		} catch (ClassNotFoundException e) {
			throw new RuntimeException("readObject can't find class "
					+ className, e);
		}
		return declaredClass;
	}

	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;
	}

	@Override
	public Configuration getConf() {
		return this.conf;
	}

}
