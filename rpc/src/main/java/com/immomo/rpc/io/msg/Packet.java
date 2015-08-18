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
 * the wrapper of message
 *
 */
public class Packet {

	/***
	 * the message header
	 */
	private Header header;

	/***
	 * the message body
	 */
	private Body body;

	public Packet() {
		this.header = new Header();
		this.body = new Body();
	}

	public Packet(final Header header, final Body body) {
		this.body = body;
		this.header = header;
	}

	public Header getHeader() {
		return header;
	}

	public void setHeader(Header header) {
		this.header = header;
	}

	public Body getBody() {
		return body;
	}

	public void setBody(Body body) {
		this.body = body;
	}

	public void writeOut(DataOutput out) throws IOException {
		this.header.write(out);
		this.body.write(out, this.header.getLength());
	}

	public void write(OutputStream out) throws IOException {
		this.header.write(out);
		this.body.write(out, this.header.getLength());
	}
	
	public void write(ByteBuf out) throws IOException {
		this.header.write(out);
		this.body.write(out, this.header.getLength());
	}

	public boolean read(DataInputStream in) throws IOException {
		in.mark(0);
		this.header.read(in);
		if (in.available() >= this.header.getLength()) {
			this.body.read(in, this.header.getLength());
			return true;
		}
		in.reset();
		return false;
	}

	public void clear() {
		this.header.clear();
		this.body.clear();
	}

}
