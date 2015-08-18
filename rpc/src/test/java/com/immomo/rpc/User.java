/**
 * 
 */
package com.immomo.rpc;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.msgpack.annotation.Message;

/**
 * @author shuaipenghou 2015年6月18日下午2:13:58
 *
 */

@Message
public class User {

	private String userName;
	private int age;
	private List<Others> list = new ArrayList<Others>();
	private Map<String, Others> map = new HashMap<String, Others>();

	public User() {
	}

	public String getUserName() {
		return userName;
	}

	public void setUserName(String userName) {
		this.userName = userName;
	}

	public int getAge() {
		return age;
	}

	public void setAge(int age) {
		this.age = age;
	}

	public List<Others> getList() {
		return list;
	}

	public void setList(List<Others> list) {
		this.list = list;
	}

	public Map<String, Others> getMap() {
		return map;
	}

	public void setMap(Map<String, Others> map) {
		this.map = map;
	}

}
