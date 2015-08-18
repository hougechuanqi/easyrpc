package com.immomo.rpc.io;

import java.io.DataInputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;

public class DataInputByteBuffer extends DataInputStream {

	private static class Buffer extends InputStream {
		private final byte[] scratch = new byte[1];
		ByteBuffer[] buffers = new ByteBuffer[0];
		int bidx, pos, length;

		@Override
		public int read() {
			if (-1 == read(scratch, 0, 1)) {
				return -1;
			}
			return scratch[0] & 0xFF;
		}

		@Override
		public int read(byte[] b, int off, int len) {
			if (bidx >= buffers.length) {
				return -1;
			}
			int cur = 0;
			do {
				int rem = Math.min(len, buffers[bidx].remaining());
				buffers[bidx].get(b, off, rem);
				cur += rem;
				off += rem;
				len -= rem;
			} while (len > 0 && ++bidx < buffers.length);
			pos += cur;
			return cur;
		}

		public void reset(ByteBuffer[] buffers) {
			bidx = pos = length = 0;
			this.buffers = buffers;
			for (ByteBuffer b : buffers) {
				length += b.remaining();
			}
		}

		public int getPosition() {
			return pos;
		}

		public int getLength() {
			return length;
		}

		public ByteBuffer[] getData() {
			return buffers;
		}
	}

	private Buffer buffers;

	public DataInputByteBuffer() {
		this(new Buffer());
	}

	private DataInputByteBuffer(Buffer buffers) {
		super(buffers);
		this.buffers = buffers;
	}

	public void reset(ByteBuffer... input) {
		buffers.reset(input);
	}

	public ByteBuffer[] getData() {
		return buffers.getData();
	}

	public int getPosition() {
		return buffers.getPosition();
	}

	public int getLength() {
		return buffers.getLength();
	}
}