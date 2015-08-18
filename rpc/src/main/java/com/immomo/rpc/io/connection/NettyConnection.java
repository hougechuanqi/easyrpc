/**
 * 
 */
package com.immomo.rpc.io.connection;

import io.netty.channel.Channel;

import java.net.InetSocketAddress;

import com.immomo.rpc.io.msg.Body;
import com.immomo.rpc.io.msg.Header;
import com.immomo.rpc.io.msg.Packet;
import com.immomo.rpc.ipc.RPCServer.Call;

/**
 * 
 *
 */
public class NettyConnection extends AbstractConneciton {

	private Channel channel;

	private String sessionId;

	@Override
	public InetSocketAddress doGetRemoteAddress() {
		InetSocketAddress inetSocketAddress = (InetSocketAddress) channel
				.remoteAddress();
		return inetSocketAddress;
	}

	@Override
	public String doGetStringId() {
		return this.sessionId;
	}

	@Override
	public boolean doSend(Packet packet) {
		this.channel.writeAndFlush(packet);
		return true;
	}

	public Channel getChannel() {
		return channel;
	}

	public void setChannel(Channel channel) {
		this.channel = channel;
	}

	public void setSessionId(String sessionId) {
		this.sessionId = sessionId;
	}

	@Override
	public boolean doClose() {
		this.channel.close().awaitUninterruptibly();
		return true;
	}

	@Override
	protected boolean doIsAlive() {
		return this.channel.isOpen();
	}

	@Override
	public void response(Call call) {
		final byte[] response = call.getRpcResponse().array();
		Header header = new Header();
		header.setLength(response.length);
		Body body = new Body();
		body.setContent(response);
		Packet packet = new Packet(header, body);
		this.send(packet);
	}

}
