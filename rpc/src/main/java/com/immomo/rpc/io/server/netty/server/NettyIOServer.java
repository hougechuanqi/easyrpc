/**
 * 
 */
package com.immomo.rpc.io.server.netty.server;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.immomo.rpc.configuration.ConfigType;
import com.immomo.rpc.configuration.Configuration;
import com.immomo.rpc.io.AbstractIOServer;
import com.immomo.rpc.io.client.netty.handler.RPCClientMessageHandler;
import com.immomo.rpc.io.server.netty.FactorialServerInitializer;

/**
 * 
 *
 */
public class NettyIOServer extends AbstractIOServer {
	private static final Log LOG = LogFactory.getLog(NettyIOServer.class);
	private ServerBootstrap b;

	private Channel serverChannel;
	EventLoopGroup workerGroup = null;
	EventLoopGroup bossGroup = null;

	@Override
	public boolean start(Configuration config) {
		final int port = config.getConfig(ConfigType.PORT);
		final Integer workers = config.getConfig(ConfigType.WORKER);
		EventLoopGroup bossGroup = new NioEventLoopGroup(1);
		EventLoopGroup workerGroup = new NioEventLoopGroup();
		try {
			ServerBootstrap b = new ServerBootstrap();
			b.group(bossGroup, workerGroup)
					.channel(NioServerSocketChannel.class)
					.handler(new LoggingHandler(LogLevel.INFO))
					.childHandler(
							new FactorialServerInitializer(this.getListener()));
			ChannelFuture f = b.bind(port).syncUninterruptibly();
			LOG.info("start io server success,listening on port="+port);
		} catch (Exception e) {
			LOG.error("start io server error:"+ExceptionUtils.getStackTrace(e));
		} finally {
		}
		return true;
	}

	@Override
	public boolean shutdown() {
		serverChannel.close().awaitUninterruptibly();
		bossGroup.shutdownGracefully();
		workerGroup.shutdownGracefully();
		return true;
	}

	@Override
	public boolean stop() {
		serverChannel.close();
		return true;
	}

}
