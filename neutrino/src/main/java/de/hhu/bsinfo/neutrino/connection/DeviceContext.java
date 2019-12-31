package de.hhu.bsinfo.neutrino.connection;

import de.hhu.bsinfo.neutrino.verbs.Context;
import de.hhu.bsinfo.neutrino.verbs.DeviceAttributes;
import de.hhu.bsinfo.neutrino.verbs.ProtectionDomain;

import java.io.IOException;

public class DeviceContext {

    private final Context context;

    private final ProtectionDomain protectionDomain;
    private final int deviceId;
    private final DeviceAttributes deviceAttributes;

    public DeviceContext(int deviceId) throws IOException {

        this.deviceId = deviceId;

        context = Context.openDevice(deviceId);
        if(context == null) {
            throw new IOException("Cannot open InfiniBand device.");
        }

        protectionDomain = context.allocateProtectionDomain();
        if(protectionDomain == null) {
            throw  new IOException("Unable to allocate protection domain");
        }

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
