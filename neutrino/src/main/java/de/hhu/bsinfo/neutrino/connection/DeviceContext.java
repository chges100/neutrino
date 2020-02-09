package de.hhu.bsinfo.neutrino.connection;

import de.hhu.bsinfo.neutrino.buffer.RegisteredBuffer;
import de.hhu.bsinfo.neutrino.verbs.AccessFlag;
import de.hhu.bsinfo.neutrino.verbs.Context;
import de.hhu.bsinfo.neutrino.verbs.DeviceAttributes;
import de.hhu.bsinfo.neutrino.verbs.ProtectionDomain;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class DeviceContext {

    private static final Logger LOGGER = LoggerFactory.getLogger(DeviceContext.class);

    private final Context context;

    private final ProtectionDomain protectionDomain;
    private final int deviceId;
    private final DeviceAttributes deviceAttributes;

    public DeviceContext(int deviceId) throws IOException {

        this.deviceId = deviceId;

        LOGGER.info("Open device context");

        context = Context.openDevice(deviceId);
        if(context == null) {
            throw new IOException("Cannot open InfiniBand device");
        }

        LOGGER.trace("Allocate protection domain");

        protectionDomain = context.allocateProtectionDomain();
        if(protectionDomain == null) {
            throw  new IOException("Unable to allocate protection domain");
        }

        LOGGER.trace("Query device attributes");

        deviceAttributes = context.queryDevice();
    }

    public RegisteredBuffer allocRegisteredBuffer(long size) {
        LOGGER.trace("Allocate new memory region for device {} of size {}", deviceId, size);

        return protectionDomain.allocateMemory(size, AccessFlag.LOCAL_WRITE, AccessFlag.REMOTE_READ, AccessFlag.REMOTE_WRITE, AccessFlag.MW_BIND);
    }

    public ProtectionDomain getProtectionDomain() {
        return protectionDomain;
    }

    public int getDeviceId() {
        return deviceId;
    }

    public DeviceAttributes getDeviceAttributes() {
        return deviceAttributes;
    }

    public Context getContext() {
        return context;
    }


}
