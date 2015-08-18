package com.immomo.rpc;

import java.io.IOException;

import com.immomo.rpc.configuration.ConfigType;
import com.immomo.rpc.configuration.Configuration;
import com.immomo.rpc.exception.RPCParamterErrorException;
import com.immomo.rpc.ipc.RPC;

public class Server {

	public static void main(String[] args) throws RPCParamterErrorException,
			IOException {
		Configuration conf = new Configuration();
		conf.addConfig(ConfigType.PORT, 8888);
		RPC.Server server = new RPC.Builder(conf).addProtocol(ITest.class,
				new TestImpl()).setNumHandlers(1).build();
		server.start();
	}

}
