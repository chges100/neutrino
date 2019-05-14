package de.hhu.bsinfo.rdma.data;

import java.nio.ByteBuffer;

public class NativeLong extends NativeDataType {

    public NativeLong(final ByteBuffer byteBuffer, final int offset) {
        super(byteBuffer, offset);
    }

    public void set(final long value) {
        getByteBuffer().putLong(getOffset(), value);
    }

    public long get() {
        return getByteBuffer().getLong(getOffset());
    }

    @Override
    public String toString() {
        return super.toString() + " " + get();
    }
}
