/**
 * 
 */
package com.immomo.rpc.io.server.netty.decoder;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.immomo.rpc.io.msg.Body;
import com.immomo.rpc.io.msg.Header;
import com.immomo.rpc.io.msg.IMsg;
import com.immomo.rpc.io.msg.Packet;

/**
 * 
 *
 */
public class RPCMsgDecoder extends ByteToMessageDecoder {

	private static final Log LOG = LogFactory.getLog(RPCMsgDecoder.class);
	/*
	 * (non-Javadoc)
	 * 
	 * @see io.netty.handler.codec.ByteToMessageDecoder#decode(io.netty.channel.
	 * ChannelHandlerContext, io.netty.buffer.ByteBuf, java.util.List)
	 */
	@Override
	protected void decode(ChannelHandlerContext ctx, ByteBuf in,
			List<Object> out) throws Exception {
		if (in.readableBytes() > 0) {
			Packet packet = null;
			while ((packet = decodeMessageFromBuffer(in, ctx)) != null) {
				out.add(packet);
			}
		}
	}

	/***
	 * read packet from buffer
	 * 
	 * @param in
	 *            io buffer
	 * @return message wrapper
	 */
	private Packet decodeMessageFromBuffer(ByteBuf in, ChannelHandlerContext ctx) {
		if (in.readableBytes() < IMsg.STAMPLEN) {
			return null;
		}
		in.markReaderIndex();
		final byte stamp = in.readByte();
		if (stamp != IMsg.STAMP) {
			LOG.error("error header stamp"
					+ ctx.channel().remoteAddress());
			ctx.close();
			return null;
		}
		if (in.readableBytes() < IMsg.HEADERLEN) {
			in.resetReaderIndex();
			return null;
		}
		final int length = in.readInt();// read message length from buffer
		final byte version = in.readByte();// read message version from buffer
		if (in.readableBytes() < length) {// if the readable length less than
											// length then reset
			in.resetReaderIndex();
			return null;
		}
		final byte[] content = new byte[length];
		in.readBytes(content);
		final Header header = new Header(length, version);
		final Body body = new Body(content);
		return new Packet(header, body);
	}

}
