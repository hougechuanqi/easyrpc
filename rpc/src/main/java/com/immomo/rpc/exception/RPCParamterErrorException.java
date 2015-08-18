/**
 * 
 */
package com.immomo.rpc.exception;

/**
 * @author shuaipenghou 2015年6月2日下午2:38:39
 *
 */
public class RPCParamterErrorException extends IllegalArgumentException {

	private static final long serialVersionUID = 1L;

	/**
	 * Constructs exception with the specified detail message.
	 * 
	 * @param message
	 *            detailed message.
	 */
	public RPCParamterErrorException(final String message) {
		super(message);
	}

}
