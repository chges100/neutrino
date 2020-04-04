package de.hhu.bsinfo.neutrino.buffer;

import de.hhu.bsinfo.neutrino.connection.ReliableConnection;
import de.hhu.bsinfo.neutrino.verbs.QueuePair;
import de.hhu.bsinfo.neutrino.verbs.SendWorkRequest;
import de.hhu.bsinfo.neutrino.verbs.SendWorkRequest.OpCode;
import de.hhu.bsinfo.neutrino.verbs.SendWorkRequest.SendFlag;

import java.io.IOException;

public class RemoteBuffer {

    private final ReliableConnection reliableConnection;
    private final long address;
    private final long capacity;
    private final int key;

    public RemoteBuffer(ReliableConnection reliableConnection, long address, long capacity, int key) {
        this.reliableConnection = reliableConnection;
        this.address = address;
        this.capacity = capacity;
        this.key = key;
    }

    public RemoteBuffer(ReliableConnection reliableConnection, BufferInformation bufferInformation) {
        this.reliableConnection = reliableConnection;
        this.address = bufferInformation.getAddress();
        this.capacity = bufferInformation.getCapacity();
        this.key = bufferInformation.getRemoteKey();
    }

    public long read(RegisteredBuffer localBuffer) {
        return execute(OpCode.RDMA_READ, localBuffer);
    }

    public long read(long index, RegisteredBuffer buffer, long offset, long length) {
        return execute(OpCode.RDMA_READ, index, buffer, offset, length);
    }

    public long write(RegisteredBuffer localBuffer) throws IOException {
        return execute(OpCode.RDMA_WRITE, localBuffer);
    }

    public long write(long index, RegisteredBuffer buffer, long offset, long length) {
        return execute(OpCode.RDMA_WRITE, index, buffer, offset, length);
    }

    private long execute(final OpCode operation, RegisteredBuffer buffer) {
        return execute(operation, 0, buffer, 0, buffer.capacity());
    }

    private long execute(final OpCode operation, long index, RegisteredBuffer buffer, long offset, long length)  {
        long ret = 0;

        try {
            ret = reliableConnection.execute(buffer, operation, offset, length, this.address, this.key, index);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return ret;

    }

    public long capacity() {
        return capacity;
    }

    @Override
    public String toString() {
        return "RemoteBuffer {" +
            ",\n\taddress=" + address +
            ",\n\tcapacity=" + capacity +
            ",\n\tkey=" + key +
            "\n}";
    }
}
