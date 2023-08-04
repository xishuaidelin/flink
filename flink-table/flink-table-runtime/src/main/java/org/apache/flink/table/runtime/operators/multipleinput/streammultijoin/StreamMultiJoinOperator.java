/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.flink.table.runtime.operators.multipleinput.streammultijoin;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorV2;
import org.apache.flink.streaming.api.operators.Input;
import org.apache.flink.streaming.api.operators.MultipleInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.utils.MultiJoinedRowData;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.operators.join.JoinConditionWithNullFilters;
import org.apache.flink.table.runtime.operators.join.stream.state.JoinInputSideSpec;
import org.apache.flink.table.runtime.operators.join.stream.state.JoinRecordStateView;
import org.apache.flink.table.runtime.operators.multipleinput.input.InputSpec;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;

import java.util.List;
import java.util.Map;

/**
 * Now the operator works for broadcast multi-join with all inputs are broadcast except two input
 *
 * whose data size is big. Stream multi Join operator which supports INNER/LEFT/RIGHT/FULL JOIN with
 * multiple input. * Suppose that there are all broadcast inputs except two input. Here the most
 * important thing is to represent * the relation of multi input for joining. * *
 * inputEdge.outputType -> InternalTypeInfo(*TypeInfo) -> for nullPadding and MapState * * required
 * things: * 1. specify if input is broadcast or major to initialize the state * 2.
 */
public class StreamMultiJoinOperator extends AbstractStreamOperatorV2<RowData>
        implements MultipleInputStreamOperator<RowData> {

    private static final long serialVersionUID = -376944622236540125L;

//    just for naming the joinStateView
//    protected static final String LEFT_RECORDS_STATE_NAME = "left-records";
//    protected static final String RIGHT_RECORDS_STATE_NAME = "right-records";
    private final List<InputSpec> inputSpecs;

    private final List<Input<StreamRecord<RowData>>> inputs;

    // broadcastStateDescriptors ( broadcast inputs >=1 )
    private final List<MapStateDescriptor<?, ?>> broadcastStateDescriptors;

    private transient Map<MapStateDescriptor<?, ?>, BroadcastState<?, ?>> broadcastStates;

    private final Map<Integer, InputSpec> inputSpecMap;

    // for master joining, no need for broadcast state and the size of retention time <=2
    protected final long stateRetentionTimes[];

    private final boolean[] filterNullKeys;

    // joinCondition using codegen
    private final GeneratedJoinCondition[] generatedJoinConditions;
    protected transient JoinConditionWithNullFilters[] joinConditions;

    // for recording the multi input joinSpecs
    protected final JoinInputSideSpec[] joinInputSideSpecs;

    // for nullPadding and mapState
    protected final InternalTypeInfo<RowData>[] typeInfos;

    // for collecting the output data using out in AbstractStreamOpV2 for initializing
    protected transient TimestampedCollector<RowData> collector;

    // for store input's state that is not broadcast input
    private transient JoinRecordStateView[]  recordStateViews;

    private transient MultiJoinedRowData outRow;

    // follow the order to define the join spec is outer or not.
    // private final boolean[] isOuters;

    // for padding RowData and it need to match the row size for each input row
    private transient RowData[] nullRows;

    private transient joinRelation

    public StreamMultiJoinOperator(
            StreamOperatorParameters<RowData> parameters,
            List<InputSpec> inputSpecs,
            List<Input<StreamRecord<RowData>>> inputs,
//          List<MapStateDescriptor<?, ?>> broadcastStateDescriptors,
//          Map<Integer, InputSpec> inputSpecMap,
            long[] stateRetentionTime,
            boolean[] filterNullKeys,
            GeneratedJoinCondition[] generatedJoinConditions,
            JoinInputSideSpec[] joinInputSideSpecs,
            InternalTypeInfo<RowData>[] typeInfos) {
        super(parameters, inputSpecs.size());
        this.inputs = inputs;
//        this.broadcastStateDescriptors = broadcastStateDescriptors;
//        this.inputSpecMap = inputSpecMap;
        this.stateRetentionTimes = stateRetentionTime;
        this.filterNullKeys = filterNullKeys;
        this.generatedJoinConditions = generatedJoinConditions;
        this.joinInputSideSpecs = joinInputSideSpecs;
        this.typeInfos = typeInfos;
        this.inputSpecs = inputSpecs;
    }

    @Override
    public void open() throws Exception {
        super.open();
        for( GeneratedJoinCondition condition : generatedJoinConditions){
            // JoinCondition condition =
            //         generatedJoinCondition.newInstance(getRuntimeContext().getUserCodeClassLoader());
            // joinCondition.add( new JoinConditionWithNullFilters(condition, filterNullKeys, this));
        }
        // initialize the broadcast state


        // this.joinCondition.setRuntimeContext(getRuntimeContext());
        // this.joinCondition.open(new Configuration());
        //
        // this.collector = new TimestampedCollector<>(output);
    }

    public List<?> resolveJoinOrder(){


        return null;
    }

    public void metaJoin() throws Exception{

    }

    // execute the metaJoin in order from result of resolveJoinOrder
    public void executeMultiJoin(){

    }

    // called in joinInput no need call here
    //    public void processElement(int inputId) throws Exception {
    //        List<?> order = resolveJoinOrder();
    //        //
    //        executeMultiJoin();
    //    }

    public void addInput(Input<StreamRecord<RowData>> input) {
        inputs.add(input);

    }

    @Override
    public List<Input> getInputs() {
        return null;
    }

}
