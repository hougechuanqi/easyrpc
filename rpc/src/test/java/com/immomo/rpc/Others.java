package com.immomo.rpc;

import org.msgpack.annotation.Message;

@Message
public class Others {
	private String message;
	private int age;
	public String getMessage() {
		return message;
	}
	public void setMessage(String message) {
		this.message = message;
	}
	public int getAge() {
		return age;
	}
	public void setAge(int age) {
		this.age = age;
	}
	
}
