package com.immomo.rpc;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import com.immomo.rpc.util.MsgPackUtils;

public class Test {

	public static void main(String[] args) throws IOException {
		ByteArrayOutputStream ping = new ByteArrayOutputStream();
		User user=new User();
		Others o=new Others();
		o.setMessage("hello");
		o.setAge(11);
		user.setAge(1);
		user.setUserName("houge");
		user.setAge(10);
		user.setUserName("中古");
		user.getList().add(o);
		user.getMap().put("111", o);
		MsgPackUtils.writeObject(ping, user);
		byte[] bytes=ping.toByteArray();
		ByteArrayInputStream in=new ByteArrayInputStream(bytes);
		User uu=MsgPackUtils.readObject(in, User.class);
		System.out.println(uu);

	}

}
