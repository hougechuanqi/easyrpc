/**
 * 
 */
package com.immomo.rpc;

import java.io.IOException;
import java.util.List;

import com.immomo.rpc.configuration.Configuration;
import com.immomo.rpc.ipc.RPC;

/**
 * @author shuaipenghou 2015年6月17日上午11:05:27
 *
 */
public class Client {

	public static void main(String args[]) throws IOException,
			InstantiationException, IllegalAccessException,
			ClassNotFoundException {
		ITest test = RPC.getProxy(ITest.class, "127.0.0.1", 8888,
				new Configuration());
		User user = new User();
		user.setAge(10);
		user.setUserName("中古");
		List<User> list = test.syncUser(user);
		System.out.println(user.getUserName() + "=" + user.getAge());
		test = RPC
				.getProxy(ITest.class, "127.0.0.1", 8888, new Configuration());
		long start = System.nanoTime();
		for (int i = 0; i < 10000; i++) {
			List<User> userList=test.syncUser(user);
			user=userList.get(0);
		}
		long cost = System.nanoTime() - start;

		System.out.println("total=" + cost + "     avarage=" + cost / 10000
				+ " 纳秒");
		System.out.println(user.getAge());
		test = new TestImpl();
		start = System.nanoTime();
		for (int i = 0; i < 10000; i++) {
			test.syncUser(user);
		}
		cost = System.nanoTime() - start;

		System.out.println("total=" + cost + "     avarage=" + cost / 10000
				+ " 纳秒");
		System.out.println(user.getAge());

		// ByteArrayOutputStream out = new ByteArrayOutputStream();
		// List<Others> list = new ArrayList<Others>();
		// list.add(new Others());
		// Map<String, Others> map = new HashMap<String, Others>();
		// map.put("1", new Others());
		// List<String> list = new ArrayList<String>();
		// Class clazzList = list.getClass();
		// System.out.println(clazzList);
		// List<User> userList = new LinkedList<User>();
		// userList.add(user);
		// Class userClazz = userList.getClass();
		// System.out.println(userClazz);
		// Class superClazz = List.class;
		//
		// ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
		// DataOutputStream out = new DataOutputStream(byteOut);
		// MsgPackUtils.writeList(out, userList);
		// final byte[] arrays = byteOut.toByteArray();
		// ByteArrayInputStream in = new ByteArrayInputStream(arrays);
		// DataInputStream input = new DataInputStream(in);
		// Object obj = MsgPackUtils.readList(input);
		// System.out.println(obj);
		//
		// Map<String, User> map = new HashMap<String, User>();
		// map.put("111", user);
		// byteOut.reset();
		// MsgPackUtils.writeMap(out, map);
		// final byte[] arr = byteOut.toByteArray();
		// in = new ByteArrayInputStream(arr);
		// input = new DataInputStream(in);
		// Map mappp = MsgPackUtils.readMap(input);
		//
		// System.out.println(mappp);

	}
}
