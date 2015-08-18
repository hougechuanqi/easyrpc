/**
 * 
 */
package com.immomo.rpc;

import java.util.ArrayList;
import java.util.List;

/**
 * @author shuaipenghou 2015年6月17日上午11:02:07
 *
 */
public class TestImpl implements ITest {

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.immomo.rpc.ITest#sayHello(java.lang.String)
	 */
	@Override
	public String sayHello(String hello) {
		System.out.println(hello + "RPC invoke success!!!");
		return hello;
	}

	@Override
	public List<User> syncUser(User user) {

//		System.out.println("receive RPC!!!" + user.getAge() + ":"
//				+ user.getUserName());
		List<User> list = new ArrayList<User>();
		user.setAge(user.getAge() + 1);
		list.add(user);
		return list;
	}

}
