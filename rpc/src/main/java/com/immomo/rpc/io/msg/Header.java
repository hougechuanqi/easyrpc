/**
 * 
 */
package com.immomo.rpc.io.msg;

import io.netty.buffer.ByteBuf;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * RPC Request Header
 *
 */
public class Header implements IMsg {

	private byte stamp = STAMP;

	/***
	 * define the message length
	 */
	private int length;

	/***
	 * define the message version,default is 0
	 */
	private byte version = 0;

	public Header() {
	}

	public Header(final int length, final byte version) {
		this.length = length;
		this.version = version;
	}

	public Header(final int length) {
		this.length = length;
	}

	public int getLength() {
		return length;
	}

	public void setLength(int length) {
		this.length = length;
	}

	public byte getVersion() {
		return version;
	}

	public void setVersion(byte version) {
		this.version = version;
	}

	public byte getStamp() {
		return stamp;
	}

	public void setStamp(byte stamp) {
		this.stamp = stamp;
	}

	public void write(DataOutput out) throws IOException {
		out.writeByte(getStamp());
		out.writeInt(getLength());
		out.writeByte(getVersion());
	}

	public void write(OutputStream out) throws IOException {
		out.write(getStamp());
		out.write(getLength());
		out.write(getVersion());
	}
	
	public void write(ByteBuf out){
		out.writeByte(getStamp());
		out.writeInt(getLength());
		out.writeByte(getVersion());
	}

	public void read(DataInputStream in) throws IOException {
		this.stamp = in.readByte();
		this.length = in.read();
		this.version = in.readByte();
	}

	public void clear() {
		this.length = 0;
	}
}
