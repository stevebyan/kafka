package org.apache.kafka.clients;

import com.ibm.disni.verbs.IbvSendWR;
import com.ibm.disni.verbs.IbvSendWR.Rdma;
import com.ibm.disni.verbs.IbvSge;

import java.nio.ByteBuffer;
import java.util.LinkedList;


public class FetchRDMASlotRequest implements RDMAWrBuilder {

    private long remoteAddress;
    private int rkey;
    private int length;

    private ByteBuffer targetBuffer;
    private int lkey;

    public FetchRDMASlotRequest(long remoteAddress, int rkey, int length, ByteBuffer targetBuffer, int lkey){
        this.remoteAddress = remoteAddress;
        this.rkey = rkey;
        this.length = length;
        this.targetBuffer = targetBuffer;
        this.lkey = lkey;
    }

    @Override
    public LinkedList<IbvSendWR> build() {
        LinkedList<IbvSendWR> wrs = new LinkedList<>();

        IbvSge sgeSend = new IbvSge();
        sgeSend.setAddr(((sun.nio.ch.DirectBuffer) targetBuffer).address());
        sgeSend.setLength(length);
        sgeSend.setLkey(lkey);
        LinkedList<IbvSge> sgeList = new LinkedList<>();
        sgeList.add(sgeSend);


        IbvSendWR sendWR = new IbvSendWR( );
        //sendWR.setWr_id(1002);
        sendWR.setSg_list(sgeList);
        sendWR.setOpcode(IbvSendWR.IBV_WR_RDMA_READ);
        sendWR.setSend_flags(IbvSendWR.IBV_SEND_SIGNALED);


        Rdma rdmapart = sendWR.getRdma();
        rdmapart.setRemote_addr(remoteAddress);
        rdmapart.setRkey(rkey);

        wrs.add(sendWR);

        return wrs;
    }

    @Override
    public ByteBuffer getTargetBuffer() {
        return targetBuffer;
    }
}
