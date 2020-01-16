package org.apache.kafka.clients;

import com.ibm.disni.verbs.*;

import java.util.LinkedList;
import java.util.List;

public class SimpleVerbsEP {

    private final RdmaCmId id;
    private final IbvQP qp;
    private final IbvCQ sendcq;
    private final IbvCQ recvcq;


    public final int CanPostReceives;

    private int canIssueRdma;
    private int canSendRequests;

    private LinkedList<IbvSendWR> delayedWrites = new LinkedList<IbvSendWR>();


    private final IbvWC[] sendwcList;
    private final IbvWC[] recvwcList;


    SVCPollCq sendpoll = null;
    SVCPollCq recvpoll = null;

    SVCPostRecv emptyrecv = null;

    private final  int contentedLimit;

    SimpleVerbsEP(RdmaCmId id, IbvQP qp, IbvCQ sendcq, IbvCQ recvcq, int maxRecvSize,
                  int maxSendSize, int requestQuota, int wcBatch, int contentedLimit){
        this.id = id;
        this.qp = qp;
        this.sendcq = sendcq;
        this.recvcq = recvcq;
        this.canIssueRdma = maxSendSize;
        this.canSendRequests = requestQuota;
        this.CanPostReceives = maxRecvSize;
        sendwcList = new IbvWC[wcBatch];
        recvwcList = new IbvWC[wcBatch];
        for (int i = 0; i < sendwcList.length; i++){
            sendwcList[i] = new IbvWC();
        }
        for (int i = 0; i < recvwcList.length; i++){
            recvwcList[i] = new IbvWC();
        }
        this.contentedLimit= contentedLimit;

    }

    public boolean ready() { return true; }

    public int getQPnum() throws Exception { return qp.getQp_num(); }

    public void postsend(IbvSendWR wr, boolean dosend) throws Exception{
        delayedWrites.add(wr);

        if(dosend) {
            trigger_send();
        }
    }

    void trigger_send() throws Exception{

        int canSend = Math.min(canIssueRdma,canSendRequests );
        if(canSend==0)
            return;

        int willSend = Math.min(canSend, delayedWrites.size());

        if(willSend == 0)
            return;


        LinkedList<IbvSendWR> wrList_tosend =  new LinkedList<>();

        for(int i =0; i < willSend; i++){
            wrList_tosend.add(delayedWrites.pollFirst());
        }

        canIssueRdma-=willSend;
        canSendRequests-=willSend;

        qp.postSend(wrList_tosend,null).execute().free();
    }



    /*
        We use stateful poll request to reduce overhead of polling.
        Warning! IbvWC must be processed before polling again!

     */
    public LinkedList<IbvWC> pollsend( ) throws Exception{
        LinkedList<IbvWC> recvList =  new LinkedList<>();
        pollsend(recvList);
        return recvList;
    }

    public int pollsend(LinkedList<IbvWC> recvList) throws Exception{
        if(this.sendpoll == null) {
            this.sendpoll = sendcq.poll(sendwcList, sendwcList.length);
        }
        int compl = this.sendpoll.execute().getPolls();
        for(int i = 0; i < compl; i++){
            if(sendwcList[i].getOpcode() == IbvWC.IbvWcOpcode.IBV_WC_RDMA_READ.getOpcode()){
                canSendRequests++;
            }
            recvList.add(sendwcList[i]);
            canIssueRdma++;
        }

        return compl;
    }

    public boolean hasUnsentRequests(){
        return !delayedWrites.isEmpty();
    }

    public boolean isContented( ){
        return delayedWrites.size() > this.contentedLimit;
    }



    public LinkedList<IbvWC> pollrecv( ) throws Exception{
        LinkedList<IbvWC> recvList =  new LinkedList<>();
        pollrecv(recvList);
        return recvList;
    }

    public int pollrecv(LinkedList<IbvWC> recvList) throws Exception{
        if(this.recvpoll == null) {
            this.recvpoll = recvcq.poll(recvwcList, sendwcList.length);
        }
        int compl = this.recvpoll.execute().getPolls();
        for(int i = 0; i < compl; i++){
            recvList.add(recvwcList[i]);
            canSendRequests++;
        }
        return compl;
    }



    public void postEmptyRecv() throws Exception{
        if(emptyrecv == null){
            IbvRecvWR wr = new IbvRecvWR();
            List<IbvRecvWR>  list_wr = new LinkedList<>();
            list_wr.add(wr);
            emptyrecv = qp.postRecv(list_wr, null);
        }

        emptyrecv.execute();
    }
}
