/**
 * 
 */
package com.immomo.rpc;

import java.util.List;

/**
 * @author shuaipenghou 2015年6月17日上午11:01:37
 *
 */
public interface ITest {

	public String sayHello(String hello);
	
	public List<User> syncUser(User user);

}
