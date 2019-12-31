package de.hhu.bsinfo.neutrino.connection;

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

        LOGGER.info("Allocate protection domain");

        protectionDomain = context.allocateProtectionDomain();
        if(protectionDomain == null) {
            throw  new IOException("Unable to allocate protection domain");
        }

        LOGGER.info("Query device attributes");

        deviceAttributes = context.queryDevice();
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
