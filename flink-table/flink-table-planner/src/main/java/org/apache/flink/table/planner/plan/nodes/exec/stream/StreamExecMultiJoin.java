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

package org.apache.flink.table.planner.plan.nodes.exec.stream;

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;

import org.apache.flink.streaming.api.transformations.KeyedBroadcastStateMultipleInputTransformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.SingleTransformationTranslator;
import org.apache.flink.table.planner.plan.nodes.exec.StateMetadata;
import org.apache.flink.table.planner.plan.nodes.exec.spec.JoinSpec;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.planner.plan.nodes.exec.utils.TransformationMetadata;
import org.apache.flink.table.planner.plan.utils.JoinUtil;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.operators.join.stream.AbstractStreamingJoinOperator;
import org.apache.flink.table.runtime.operators.multipleinput.streammultijoin.StreamMultiJoinOperator;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * Now suppose that the relNodes would recognize the relationships between multiple tables pass it to execNode.
 *
 * {@link StreamExecNode} for multi Joins.
 *
 * <p>multi join for cases that sql hint points to broadcast join.
 *    There are all broadcast input except two input. For simple case, we could just consider three-way multi-join.
 */
public class StreamExecMultiJoin extends ExecNodeBase<RowData>
        implements StreamExecNode<RowData>, SingleTransformationTranslator<RowData> {

    public static final String JOIN_TRANSFORMATION = "multiJoin";

    public static final String LEFT_STATE_NAME = "leftMajorState";

    public static final String RIGHT_STATE_NAME = "rightMajorState";

    private final JoinSpec[] joinSpecs;

    private final List<int[]> leftUpsertKeys;

    private final List<int[]> rightUpsertKeys;

    @Nullable
    private final List<StateMetadata> stateMetadataList;

    protected StreamExecMultiJoin(
            ReadableConfig persistedConfig,
            JoinSpec[] joinSpec,
            List<int[]> leftUpsertKeys,
            List<int[]> rightUpsertKeys,
            List<InputProperty> inputProperties,
            LogicalType outputType,
            String description) {
        super(
                ExecNodeContext.newNodeId(),
                ExecNodeContext.newContext(StreamExecMultiJoin.class),
                persistedConfig,
                inputProperties,
                outputType,
                description);
        this.joinSpecs = joinSpec;
        this.leftUpsertKeys = leftUpsertKeys;
        this.rightUpsertKeys = rightUpsertKeys;
        this.stateMetadataList = StateMetadata.getMultiInputOperatorDefaultMeta(
                persistedConfig, LEFT_STATE_NAME, RIGHT_STATE_NAME);

    }

    /**
     * Internal method, translates this node into a Flink operator.
     *
     * @param planner The planner.
     * @param config  per-{@link ExecNode} configuration that contains the merged configuration from
     *                various layers which all the nodes implementing this method should use, instead of
     *                retrieving configuration from the {@code planner}. For more details check {@link
     *                ExecNodeConfig}.
     */
    @Override
    protected Transformation<RowData> translateToPlanInternal(
            PlannerBase planner,
            ExecNodeConfig config) {

        // get the inputs from physical relNodes and the first two is major input for joining
        final List<ExecEdge> inputEdges = getInputEdges();

        final List<Transformation<RowData>> transformations = new ArrayList<>();

        final List<RowType> outTypes = new ArrayList<>();

        for (ExecEdge edge : inputEdges){
            transformations.add((Transformation<RowData>) edge.translateToPlan(planner));
            outTypes.add( (RowType) edge.getOutputType());
        }

        //  for major input to keep state retention
        List<Long> leftAndRightStateRetentionTime =
                StateMetadata.getStateTtlForMultiInputOperator(config, 2, stateMetadataList);
        long leftStateRetentionTime = leftAndRightStateRetentionTime.get(0);
        long rightStateRetentionTime = leftAndRightStateRetentionTime.get(1);

        // wait to get MultiJoinSpec for create conditions
        List<GeneratedJoinCondition> generatedConditions = new ArrayList<>();

        StreamMultiJoinOperator operator =
                new StreamMultiJoinOperator(

                );


        final KeyedBroadcastStateMultipleInputTransformation<RowData> transform  = null;

//        transform = ExecNodeUtil.createKeyedBroadcastStateMultipleInputTransformation(
//                        createTransformationMeta(JOIN_TRANSFORMATION, config),
//                        operator,
//                        TypeInformation<O> outputType,
//                        int parallelism,
//                        long memoryBytes,
//                        TypeInformation<?> stateKeyType,
//                        List<MapStateDescriptor<?, ?>> broadcastStateDescriptors,
//                        int numNonBcInputs
//                );

        return  transform;


    }
}
