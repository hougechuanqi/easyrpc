package com.immomo.rpc.io.server.netty.handler;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.AttributeKey;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.immomo.rpc.io.IPacketListener;
import com.immomo.rpc.io.PacketWrapper;
import com.immomo.rpc.io.client.netty.handler.RPCClientMessageHandler;
import com.immomo.rpc.io.connection.IConnection;
import com.immomo.rpc.io.connection.NettyConnection;
import com.immomo.rpc.io.msg.Packet;
import com.immomo.rpc.io.pool.PacketWrapperPool;
import com.immomo.rpc.ipc.Constants;
import com.immomo.rpc.util.Util;

@Sharable
public class RPCMessageHandler extends ChannelInboundHandlerAdapter {
	private static final Log LOG = LogFactory.getLog(RPCMessageHandler.class);
	private IPacketListener listener;

	public RPCMessageHandler(IPacketListener listener) {
		this.listener = listener;
	}

	@Override
	public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
		NettyConnection conn = (NettyConnection) ctx.channel().attr(Constants.CONN).get();
		listener.onClose(conn);
	}

	@Override
	public void channelActive(ChannelHandlerContext ctx) throws Exception {
		NettyConnection conn = new NettyConnection();
		conn.setChannel(ctx.channel());
		conn.setSessionId(Util.createSessionId());
		Channel channel = ctx.channel();
		channel.attr(Constants.CONN).set(
				conn);
		listener.onConnect(conn);
	}

	@Override
	public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
		// TODO Auto-generated method stub
		super.channelRegistered(ctx);
	}

	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		// TODO Auto-generated method stub
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
	public void userEventTriggered(ChannelHandlerContext ctx, Object evt)
			throws Exception {
		// TODO Auto-generated method stub
		super.userEventTriggered(ctx, evt);
	}

	@Override
	public void channelWritabilityChanged(ChannelHandlerContext ctx)
			throws Exception {
		// TODO Auto-generated method stub
		super.channelWritabilityChanged(ctx);
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
			throws Exception {
		// TODO Auto-generated method stub
		super.exceptionCaught(ctx, cause);
	}

}
