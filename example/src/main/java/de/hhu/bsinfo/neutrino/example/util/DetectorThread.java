package de.hhu.bsinfo.neutrino.example.util;

import de.hhu.bsinfo.jdetector.lib.IbFabric;
import de.hhu.bsinfo.jdetector.lib.IbNode;
import de.hhu.bsinfo.jdetector.lib.IbPort;
import de.hhu.bsinfo.jdetector.lib.exception.IbFileException;
import de.hhu.bsinfo.jdetector.lib.exception.IbMadException;
import de.hhu.bsinfo.jdetector.lib.exception.IbNetDiscException;
import de.hhu.bsinfo.jdetector.lib.exception.IbVerbsException;
import de.hhu.bsinfo.neutrino.connection.ReliableConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DetectorThread extends Thread{
    private static final Logger LOGGER = LoggerFactory.getLogger(DetectorThread.class);

    private static final boolean network = false;
    private static final boolean compat = true;

    private IbFabric fabric = null;
    private final int port;

    public DetectorThread(int port) {
        this.port = port;

        try {
            fabric = new IbFabric(network, compat);
        } catch (IbFileException | IbMadException | IbVerbsException | IbNetDiscException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run() {
        for(IbNode node : fabric.getNodes()) {
            for(IbPort port : node.getPorts()) {
                System.out.println(node.getDescription() + " (port " + port.getNum() + "): " + port.getXmitDataBytes() + " Bytes");
            }
        }

    }



}
