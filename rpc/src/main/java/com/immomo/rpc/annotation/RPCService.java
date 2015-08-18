/**
 * 
 */
package com.immomo.rpc.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Documented
@Retention(RetentionPolicy.RUNTIME)
public @interface RPCService {

	public String serviceName() default "";

}
