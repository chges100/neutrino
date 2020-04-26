package de.hhu.bsinfo.neutrino.connection;

import de.hhu.bsinfo.neutrino.buffer.RegisteredBuffer;
import de.hhu.bsinfo.neutrino.verbs.AccessFlag;
import de.hhu.bsinfo.neutrino.verbs.Context;
import de.hhu.bsinfo.neutrino.verbs.DeviceAttributes;
import de.hhu.bsinfo.neutrino.verbs.ProtectionDomain;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * This class represents a device context, i.e. device id, context, protection domain and the device attributes.
 *
 * @author Christian Gesse
 */
public class DeviceContext {

    private static final Logger LOGGER = LoggerFactory.getLogger(DeviceContext.class);

    private final Context context;

    private final ProtectionDomain protectionDomain;
    private final int deviceId;
    private final DeviceAttributes deviceAttributes;

    /**
     * Constructor
     *
     * @param deviceId the device id
     * @throws IOException thrown if initializing failed
     */
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

    /**
     * Alloc a registered buffer corresponding to this device context.
     * This buffer can be used for work requests.
     *
     * @param size size of the buffer
     * @return the allocated buffer
     */
    public RegisteredBuffer allocRegisteredBuffer(long size) {
        LOGGER.trace("Allocate new memory region for device {} of size {}", deviceId, size);

        // registered buffers have to be allocated using ib-verbs
        return protectionDomain.allocateMemory(size, AccessFlag.LOCAL_WRITE, AccessFlag.REMOTE_READ, AccessFlag.REMOTE_WRITE, AccessFlag.MW_BIND);
    }

    /**
     * Returns protection domain.
     *
     * @return the protection domain
     */
    public ProtectionDomain getProtectionDomain() {
        return protectionDomain;
    }

    /**
     * Returns device id.
     *
     * @return the device id
     */
    public int getDeviceId() {
        return deviceId;
    }

    /**
     * Returns device attributes.
     *
     * @return the device attributes
     */
    public DeviceAttributes getDeviceAttributes() {
        return deviceAttributes;
    }

    /**
     * Returns context.
     *
     * @return the context
     */
    public Context getContext() {
        return context;
    }


}
