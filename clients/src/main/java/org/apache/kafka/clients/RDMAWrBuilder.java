package org.apache.kafka.clients;

import com.ibm.disni.verbs.*;

import java.util.LinkedList;
import java.nio.ByteBuffer;

public interface RDMAWrBuilder {

    LinkedList<IbvSendWR> build();

    ByteBuffer getTargetBuffer();

}
