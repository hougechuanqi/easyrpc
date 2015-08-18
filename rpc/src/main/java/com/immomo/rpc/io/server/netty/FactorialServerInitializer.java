package com.immomo.rpc.io.server.netty;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;

import com.immomo.rpc.io.IPacketListener;
import com.immomo.rpc.io.server.netty.coder.RPCMsgEncoder;
import com.immomo.rpc.io.server.netty.decoder.RPCMsgDecoder;
import com.immomo.rpc.io.server.netty.handler.RPCMessageHandler;

public class FactorialServerInitializer
		extends
			ChannelInitializer<SocketChannel> {

	private IPacketListener listener;


	public FactorialServerInitializer(IPacketListener listener) {
		this.listener = listener;
	}

	@Override
	public void initChannel(SocketChannel ch) {
		 ChannelPipeline pipeline = ch.pipeline();
		 pipeline.addLast( new RPCMsgDecoder());
		 pipeline.addLast( new RPCMsgEncoder());
		 pipeline.addLast(new RPCMessageHandler(listener));
	}
}
