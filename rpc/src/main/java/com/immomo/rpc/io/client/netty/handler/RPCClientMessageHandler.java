/**
 * 
 */
package com.immomo.rpc.io.client.netty.handler;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.immomo.rpc.io.IPacketListener;
import com.immomo.rpc.io.PacketWrapper;
import com.immomo.rpc.io.connection.IConnection;
import com.immomo.rpc.io.connection.NettyConnection;
import com.immomo.rpc.io.msg.Packet;
import com.immomo.rpc.io.pool.PacketWrapperPool;
import com.immomo.rpc.ipc.Constants;
import com.immomo.rpc.ipc.MsgPackRPCEngine;
import com.immomo.rpc.util.Util;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.AttributeKey;

/**
 * @author shuaipenghou 2015年6月17日下午5:56:39
 *
 */
public class RPCClientMessageHandler extends ChannelInboundHandlerAdapter {
	private static final Log LOG = LogFactory.getLog(RPCClientMessageHandler.class);
	private IPacketListener listener;
	
	private AttributeKey<NettyConnection> connAttr;
	
	public RPCClientMessageHandler(IPacketListener listener) {
		this.listener = listener;
		if(AttributeKey.exists(IConnection.CONNBINDNAME)){
			connAttr = AttributeKey.valueOf(IConnection.CONNBINDNAME);
		}else {
			connAttr = AttributeKey.newInstance(IConnection.CONNBINDNAME);
		}
	}

	@Override
	public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
		LOG.info("client close 。。。"+ctx.channel().remoteAddress());
		NettyConnection conn = (NettyConnection) ctx.attr(connAttr).get();
		listener.onClose(conn);
	}

	@Override
	public void channelActive(ChannelHandlerContext ctx) throws Exception {
		NettyConnection conn = new NettyConnection();
		
		conn.setChannel(ctx.channel());
		conn.setSessionId(Util.createSessionId());
		Channel channel = ctx.channel();
		channel.attr(connAttr).set(conn);
		listener.onConnect(conn);
	}

	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		super.channelInactive(ctx);
	}

	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg)
			throws Exception {
		PacketWrapper wrapper = null;
		try {
			final Packet packet = (Packet) msg;
			Channel channel = ctx.channel();
			NettyConnection conn = (NettyConnection) channel.attr(
					Constants.CONN).get();
			wrapper = PacketWrapperPool.borrowObject();
			wrapper.setConn(conn);
			wrapper.setPacket(packet);
			this.listener.onReceivePacket(wrapper);
		} catch (Exception e) {
			LOG.error(ExceptionUtils.getStackTrace(e));
		} finally {
			if (null != wrapper) {
				PacketWrapperPool.returnObject(wrapper);
			}
		}
	
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
			throws Exception {
		LOG.error("客户端捕捉到异常+"+cause.getMessage());
	}
	
	

}
