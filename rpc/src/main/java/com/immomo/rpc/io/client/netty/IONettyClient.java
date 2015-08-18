package com.immomo.rpc.io.client.netty;

import java.net.InetSocketAddress;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

import com.immomo.rpc.configuration.ConfigType;
import com.immomo.rpc.configuration.Configuration;
import com.immomo.rpc.configuration.ConfigurationKeys;
import com.immomo.rpc.io.AbstractIOClient;

public class IONettyClient extends AbstractIOClient {
	private static final Log LOG = LogFactory.getLog(IONettyClient.class);
	EventLoopGroup workerGroup = null;
	ChannelFuture f  = null;

	@Override
	public boolean doConnect(String host, int port, Configuration conf)
			throws Exception {
		try {
			workerGroup = new NioEventLoopGroup();
			Bootstrap b = new Bootstrap(); // (1)
			b.group(workerGroup); // (2)
			b.channel(NioSocketChannel.class); // (3)
			b.option(ChannelOption.SO_KEEPALIVE, true); // (4)
			b.option(ChannelOption.TCP_NODELAY, conf.getBoolean(
					ConfigType.TCP_NODELY,
					ConfigurationKeys.IPC_CLIENT_TCPNODELAY_DEFAULT));
			b.option(ChannelOption.SO_TIMEOUT, conf.getInt(
					ConfigType.CONNECTION_TIMEOUT,
					ConfigurationKeys.IPC_CONNECTION_TIMEOUT));
			b.handler(new FactorialClientInitializer(this.getListener()));
			// Start the client.
			f = b.connect(new InetSocketAddress(host, port))
					.syncUninterruptibly();
		} catch (Exception e) {
			LOG.error(ExceptionUtils.getStackTrace(e));
			throw e;
		}
		return true;
	}

	@Override
	public boolean doDisconnect() {
		if (workerGroup != null)
			workerGroup.shutdownGracefully();
		if (f != null)
			f.channel().closeFuture();
		return true;
	}

}
