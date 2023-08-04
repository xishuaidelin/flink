package org.apache.flink.table.runtime.operators.multipleinput.input;

import org.apache.flink.streaming.api.operators.AbstractInput;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorV2;
import org.apache.flink.streaming.api.operators.MultipleInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 *  Used for multi-join input with major input without broadcasting.
 * */
public class JoinInput<IN, OUT> extends AbstractInput<IN, OUT> {

    private transient boolean broadcast;

    public JoinInput(AbstractStreamOperatorV2<OUT> owner,
                     int inputId,
                     boolean broadcast
                    ) {
        super(owner, inputId);
        this.broadcast = broadcast;
    }

    public boolean isBroadcast(){
        return broadcast;
    }

    /**
     * Processes one element that arrived on this input of the {@link MultipleInputStreamOperator}.
     * This method is guaranteed to not be called concurrently with other methods of the operator.
     *
     * @param element
     */
    @Override
    public void processElement(StreamRecord<IN> element) throws Exception {

    }
}
