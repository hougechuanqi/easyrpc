/**
 * 
 */
package com.immomo.rpc.util;

import java.util.UUID;

/**
 * 
 *
 */
public class Util {

	public static String createSessionId() {
		UUID uuid = UUID.randomUUID();
		return uuid.toString();
	}

	public static String createCallId() {
		UUID uuid = UUID.randomUUID();
		return uuid.toString();
	}

	public static String createClientId() {
		UUID uuid = UUID.randomUUID();
		return uuid.toString();
	}

}
