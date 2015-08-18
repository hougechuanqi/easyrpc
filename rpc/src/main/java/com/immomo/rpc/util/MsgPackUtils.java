/**
 * 
 */
package com.immomo.rpc.util;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.msgpack.MessagePack;
import org.msgpack.packer.MessagePackBufferPacker;
import org.msgpack.template.Template;
import org.msgpack.template.Templates;
import org.msgpack.unpacker.MessagePackUnpacker;

/**
 * 
 *
 */
public class MsgPackUtils {

	private static final MessagePack msgpack = new MessagePack();

	public static void writrArray(DataOutput out, Object[] array)
			throws IOException {
		if (array != null) {
			writeInt(out, array.length);
			if (array.length > 0) {
				String clazzName = null;
				for (Object o : array) {
					if (o != null) {
						clazzName = o.getClass().getName();
						break;
					}
				}
				writeString(out, clazzName);
				if (clazzName != null) {
					for (Object o : array) {
						writeObject(out, o);
					}
				}
			}
		} else {
			writeInt(out, 0);
		}
	}

	public static Object readArray(DataInput in) throws IOException,
			NegativeArraySizeException, ClassNotFoundException {
		final int len = readInt(in);
		if (len > 0) {
			Class clazz = Class.forName(readString(in));
			Object obj=readObject(in, clazz);
			return obj;
		}
		return null;

	}

	public static void writeMap(DataOutput out, Map map) throws IOException {
		if (map != null) {
			writeInt(out, map.size());
			if (map.size() > 0) {
				String keyClazzName = null;
				String valueClazzName = null;
				Iterator<Object> keyIterator = map.keySet().iterator();
				while (keyIterator.hasNext()) {
					Object key = keyIterator.next();
					if (map.get(key) != null) {
						keyClazzName = key.getClass().getName();
						valueClazzName = map.get(key).getClass().getName();
						break;
					}
				}
				writeString(out, keyClazzName);
				writeString(out, valueClazzName);
				if (keyClazzName != null && valueClazzName != null) {
					writeObject(out, map);
				}
			}
		} else {
			writeInt(out, 0);
		}
	}

	public static Map<?, ?> readMap(DataInput in) throws IOException,
			ClassNotFoundException {
		Map<?, ?> map = null;
		final int len = readInt(in);
		if (len > 0) {
			final String keyClazzName = readString(in);
			final String valueClazzName = readString(in);
			if (keyClazzName != null && !"".equals(keyClazzName)
					&& valueClazzName != null && !"".equals(valueClazzName)) {
				Class<?> keyClazz = Class.forName(keyClazzName);
				Class<?> valueClazz = Class.forName(valueClazzName);
				Template<?> keyT = msgpack.lookup(keyClazz);
				Template<?> valT = msgpack.lookup(valueClazz);
				Template<?> impl = Templates.tMap(keyT, valT);
				DataInputStream input = (DataInputStream) in;
				MessagePackUnpacker unpacker = new MessagePackUnpacker(msgpack,
						input);
				map=(Map<?, ?>) unpacker.read(impl);
			}
		}
		return map;
	}

	public static void writeList(DataOutput out, List list) throws IOException {
		if (list != null) {
			writeInt(out, list.size());
			if (list.size() > 0) {
				String clazzName = null;
				for (int i = 0; i < list.size(); i++) {
					Object obj = list.get(i);
					if (obj != null) {
						clazzName = obj.getClass().getName();
						break;
					}
				}
				writeString(out, clazzName);
				if (clazzName != null)
					writeObject(out, list);
			}
		} else {
			writeInt(out, 0);
		}
	}

	public static List readList(DataInput in) throws IOException,
			ClassNotFoundException {
		List list = new ArrayList();
		final int len = readInt(in);
		if (len > 0) {
			final String clazzName = readString(in);
			if (clazzName != null && !"".equals(clazzName)) {
				Class<?> clazz = Class.forName(clazzName);
				Template<?> templ = msgpack.lookup(clazz);
				Template<?> impl = Templates.tList(templ);
				DataInputStream input = (DataInputStream) in;
				MessagePackUnpacker unpacker = new MessagePackUnpacker(msgpack,
						input);
				list = (List) unpacker.read(impl);
			}
		}
		return list;
	}

	public static void writeObject(ByteArrayOutputStream out, Object o)
			throws IOException {
		out.write(msgpack.write(o));
	}

	public static void writeObject(DataOutput out, Object o) throws IOException {
		out.write(msgpack.write(o));
	}

	@SuppressWarnings("unchecked")
	public static <T> T readObject(DataInput in, Class<?> clazz)
			throws IOException {
		DataInputStream input = (DataInputStream) in;
		return (T) msgpack.read(input,clazz);
	}

	public static <T> T readObject(DataInputStream in, Class<?> clazz)
			throws IOException {
		return (T) msgpack.read(in,clazz);
	}

	public static <T> T readObject(ByteArrayInputStream in, Class<?> clazz)
			throws IOException {
		return (T) msgpack.read(in,clazz);
	}

	public static <T> T readObject(BufferedInputStream in, Class<?> clazz)
			throws IOException {
		return (T) msgpack.read(in,clazz);
	}

	public static void writeInt(DataOutput out, int num) throws IOException {
		MessagePackBufferPacker packer = new MessagePackBufferPacker(msgpack);
		packer.write(num);
		out.write(packer.toByteArray());
	}

	public static int readInt(DataInput in) throws IOException {
		DataInputStream input = (DataInputStream) in;
		MessagePackUnpacker unpacker = new MessagePackUnpacker(msgpack, input);
		return unpacker.readInt();
	}

	public static String readString(DataInput in) throws IOException {
		DataInputStream input = (DataInputStream) in;
		MessagePackUnpacker unpacker = new MessagePackUnpacker(msgpack, input);
		return unpacker.readString();
	}

	public static void writeString(DataOutput out, String o) throws IOException {
		MessagePackBufferPacker packer = new MessagePackBufferPacker(msgpack);
		packer.write(o);
		out.write(packer.toByteArray());
	}

	public static void writeBytes(DataOutput out, byte[] bytes)
			throws IOException {
		MessagePackBufferPacker packer = new MessagePackBufferPacker(msgpack);
		packer.write(bytes);
		out.write(packer.toByteArray());
	}

	public static byte[] readBytes(DataInput in) throws IOException {
		DataInputStream input = (DataInputStream) in;
		MessagePackUnpacker unpacker = new MessagePackUnpacker(msgpack, input);
		return unpacker.readByteArray();
	}

	public static byte readByte(DataInput in) throws IOException {
		DataInputStream input = (DataInputStream) in;
		MessagePackUnpacker unpacker = new MessagePackUnpacker(msgpack, input);
		return unpacker.readByte();
	}

	public static void writeByte(DataOutput out, byte o) throws IOException {
		MessagePackBufferPacker packer = new MessagePackBufferPacker(msgpack);
		packer.write(o);
		out.write(packer.toByteArray());
	}

	public static long readLong(DataInput in) throws IOException {
		DataInputStream input = (DataInputStream) in;
		MessagePackUnpacker unpacker = new MessagePackUnpacker(msgpack, input);
		return unpacker.readLong();
	}

	public static void writeLong(DataOutput out, long o) throws IOException {
		MessagePackBufferPacker packer = new MessagePackBufferPacker(msgpack);
		packer.write(o);
		out.write(packer.toByteArray());
	}

	public static short readShort(DataInput in) throws IOException {
		DataInputStream input = (DataInputStream) in;
		MessagePack msgpack = new MessagePack();
		MessagePackUnpacker unpacker = new MessagePackUnpacker(msgpack, input);
		return unpacker.readShort();
	}

	public static void writeShort(DataOutput out, short o) throws IOException {
		MessagePackBufferPacker packer = new MessagePackBufferPacker(msgpack);
		packer.write(o);
		out.write(packer.toByteArray());
	}

	public static boolean readBoolean(DataInput in) throws IOException {
		DataInputStream input = (DataInputStream) in;
		MessagePackUnpacker unpacker = new MessagePackUnpacker(msgpack, input);
		return unpacker.readBoolean();
	}

	public static void writeBoolean(DataOutput out, boolean o)
			throws IOException {
		MessagePackBufferPacker packer = new MessagePackBufferPacker(msgpack);
		packer.write(o);
		out.write(packer.toByteArray());
	}

	public static double readDouble(DataInput in) throws IOException {
		DataInputStream input = (DataInputStream) in;
		MessagePackUnpacker unpacker = new MessagePackUnpacker(msgpack, input);
		return unpacker.readDouble();
	}

	public static void writeDouble(DataOutput out, double o) throws IOException {
		MessagePackBufferPacker packer = new MessagePackBufferPacker(msgpack);
		packer.write(o);
		out.write(packer.toByteArray());
	}

	public static float readFloat(DataInput in) throws IOException {
		DataInputStream input = (DataInputStream) in;
		MessagePackUnpacker unpacker = new MessagePackUnpacker(msgpack, input);
		return unpacker.readFloat();
	}

	public static void writeFloat(DataOutput out, float o) throws IOException {
		MessagePackBufferPacker packer = new MessagePackBufferPacker(msgpack);
		packer.write(o);
		out.write(packer.toByteArray());
	}

	public static void writeChar(DataOutput out, char o) throws IOException {
		Character charO = o;
		MessagePackBufferPacker packer = new MessagePackBufferPacker(msgpack);
		packer.write(charO);
		out.write(packer.toByteArray());
	}

	public static char readChar(DataInput in) throws IOException {
		DataInputStream input = (DataInputStream) in;
		MessagePackUnpacker unpacker = new MessagePackUnpacker(msgpack, input);
		Character charO = unpacker.read(Character.class);
		return charO;
	}

//	private static boolean hasTemplate(Class clazz) {
//		Template tmpl = msgpack.lookup(clazz);
//		if (tmpl != null)
//			return true;
//		else
//			return false;
//	}
//
//	private static void registTemplate(Class clazz) {
//		if (!hasTemplate(clazz)) {
//			msgpack.register(clazz);
//		}
//	}

}
