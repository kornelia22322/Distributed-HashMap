package map;

import org.jgroups.*;
import org.jgroups.protocols.*;
import org.jgroups.protocols.pbcast.*;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Util;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class DistributedHashMap extends ReceiverAdapter implements SimpleStringMap {
    private Map<String, String> hmap = new ConcurrentHashMap<>();
    private ProtocolStack protocolStack;
    private JChannel jChannel;

    public DistributedHashMap() throws Exception {
        System.setProperty("java.net.preferIPv4Stack", "true");

        jChannel = new JChannel(false);
        initProtocolStack();
        jChannel.setReceiver(this);
        jChannel.connect(Constants.CHANNEL);
        jChannel.getState(null, 10000);
    }

    private void initProtocolStack() throws Exception {
        protocolStack = new ProtocolStack();
        jChannel.setProtocolStack(protocolStack);
        protocolStack.addProtocol(new UDP().setValue("mcast_group_addr", InetAddress.getByName(Constants.IP_ADDRESS)))
                .addProtocol(new PING())
                .addProtocol(new MERGE3())
                .addProtocol(new FD_SOCK())
                .addProtocol(new FD_ALL().setValue("timeout", 12000).setValue("interval", 3000))
                .addProtocol(new VERIFY_SUSPECT())
                .addProtocol(new BARRIER())
                .addProtocol(new NAKACK2())
                .addProtocol(new UNICAST3())
                .addProtocol(new STABLE())
                .addProtocol(new GMS())
                .addProtocol(new UFC())
                .addProtocol(new MFC())
                .addProtocol(new STATE_TRANSFER())
                .addProtocol(new FRAG2());

        protocolStack.init();
    }


    public boolean containsKey(String key) {
        return hmap.containsKey(key);
    }

    public String get(String key) {
        return hmap.get(key);
    }

    public String put(String key, String value) {
        System.out.println("Put KEY " + key + " VALUE: " + value);
        String val = this.hmap.put(key, value);
        update(new MapMessage().setKey(key).setValue(value).setOperation(Operation.PUT));
        return val;
    }

    private void update(MapMessage mapMessage) {
        Message msg = new Message(null, null, mapMessage);
        try {
            jChannel.send(msg);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void receive(Message msg) {
        final MapMessage entry = (MapMessage) msg.getObject();
        if(entry.getOperation() == Operation.REMOVE){
            hmap.remove(entry.getKey());
        } else {
            hmap.put(entry.getKey(), entry.getValue());
        }
    }


    @Override
    public void viewAccepted(View view) {
        if(view instanceof MergeView) {
            ViewHandler handler = new ViewHandler(jChannel, (MergeView) view);
            handler.start();
        }
        System.out.println("** nodes: " + view.getMembers());
    }

    public String remove(String key) {
        String val = this.hmap.remove(key);
        System.out.println("Removed KEY " + key + " VALUE: " + val);
        if (val != null) {
            update(new MapMessage().setKey(key).setOperation(Operation.REMOVE));
        }
        return val;
    }

    public void getState(OutputStream output) throws Exception {
        synchronized(hmap) {
            Util.objectToStream(hmap, new DataOutputStream(output));
        }
    }

    public void setState(InputStream input) throws Exception {
        Map<String,String> map=(Map<String,String>)Util.objectFromStream(new DataInputStream(input));;
        synchronized(hmap) {
            hmap.clear();
            hmap.putAll(map);
        }
    }

    public void printState() {
        System.out.println("Key\tValue");
        hmap.forEach((key, value) -> {
            System.out.println(String.format("%s\t%s", key, value));
        });
    }

    public void close() throws InterruptedException {
        jChannel.close();
    }

    public void clear() {
        MapMessage message = new MapMessage().setOperation(Operation.REMOVE);
        hmap.keySet().forEach(key -> {
                message.setKey(key);
                try {
                    jChannel.send(null, message);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
            hmap.clear();
    }
}
