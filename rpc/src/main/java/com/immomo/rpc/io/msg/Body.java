/**
 * 
 */
package com.immomo.rpc.io.msg;

import io.netty.buffer.ByteBuf;

import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.io.OutputStream;

/**
 * 
 * RPC Request Body
 *
 */
public class Body implements IMsg {

	/***
	 * the message body content
	 */
	private byte[] content;

	public Body() {
	}

	public Body(final byte[] body) {
		this.content = body;
	}

	public byte[] getContent() {
		return content;
	}

	public void setContent(byte[] content) {
		this.content = content;
	}

	public void write(DataOutput out, final int length) throws IOException {
		out.write(getContent(), 0, length);
	}

	public void write(OutputStream out, final int length) throws IOException {
		out.write(getContent(), 0, length);
	}
	
	public void write(ByteBuf out, final int length) throws IOException {
		out.writeBytes(getContent(), 0, length);
	}

	public void read(DataInputStream in, final int length) throws IOException {
		this.content = new byte[length];
		in.read(content);
	}

	public void clear() {
		this.content = null;
	}
}
