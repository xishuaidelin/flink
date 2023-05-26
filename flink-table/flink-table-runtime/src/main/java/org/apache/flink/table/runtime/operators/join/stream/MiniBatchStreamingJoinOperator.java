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
 */

package org.apache.flink.table.runtime.operators.join.stream;

import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.operators.bundle.trigger.BundleTriggerCallback;
import org.apache.flink.table.runtime.operators.bundle.trigger.CoBundleTrigger;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.operators.join.stream.BatchBuffer.MiniBatchBuffer;
import org.apache.flink.table.runtime.operators.join.stream.BatchBuffer.MiniBatchBufferHasUk;
import org.apache.flink.table.runtime.operators.join.stream.BatchBuffer.MiniBatchBufferJkUk;
import org.apache.flink.table.runtime.operators.join.stream.BatchBuffer.MiniBatchBufferNoUk;
import org.apache.flink.table.runtime.operators.join.stream.state.JoinInputSideSpec;
import org.apache.flink.table.runtime.operators.join.stream.state.JoinRecordStateView;
import org.apache.flink.table.runtime.operators.metrics.SimpleGauge;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/** Streaming unbounded Join base operator which support mini-batch join. */
public abstract class MiniBatchStreamingJoinOperator extends StreamingJoinOperator
        implements BundleTriggerCallback {

    private final CoBundleTrigger<RowData, RowData> coBundleTrigger;

    private transient MiniBatchBuffer leftBuffer;
    private transient MiniBatchBuffer rightBuffer;

    private transient int leftBundleSize = 0;
    private transient int rightBundleSize = 0;
    private transient SimpleGauge<Integer> leftBundleSizeGauge;
    private transient SimpleGauge<Integer> rightBundleSizeGauge;

    public MiniBatchStreamingJoinOperator(MiniBatchStreamingJoinParameter parameter) {
        super(
                parameter.leftType,
                parameter.rightType,
                parameter.generatedJoinCondition,
                parameter.leftInputSideSpec,
                parameter.rightInputSideSpec,
                parameter.leftIsOuter,
                parameter.rightIsOuter,
                parameter.filterNullKeys,
                parameter.stateRetentionTime,
                parameter.stateRetentionTime);

        this.coBundleTrigger = parameter.coBundleTrigger;
    }

    @Override
    public void open() throws Exception {
        super.open();

        if (leftInputSideSpec.joinKeyContainsUniqueKey()) {
            this.leftBuffer = new MiniBatchBufferJkUk();
        } else if (leftInputSideSpec.hasUniqueKey()) {
            this.leftBuffer = new MiniBatchBufferHasUk();
        } else {
            this.leftBuffer = new MiniBatchBufferNoUk();
        }

        if (rightInputSideSpec.joinKeyContainsUniqueKey()) {
            this.rightBuffer = new MiniBatchBufferJkUk();
        } else if (rightInputSideSpec.hasUniqueKey()) {
            this.rightBuffer = new MiniBatchBufferHasUk();
        } else {
            this.rightBuffer = new MiniBatchBufferNoUk();
        }

        coBundleTrigger.registerCallback(this);
        coBundleTrigger.reset();
        LOG.info("Initialize MiniBatchStreamingJoinOperator successfully.");

        // register metrics
        leftBundleSizeGauge = new SimpleGauge<>(leftBundleSize);
        rightBundleSizeGauge = new SimpleGauge<>(rightBundleSize);

        getRuntimeContext().getMetricGroup().gauge("leftBundleSize", leftBundleSizeGauge);
        getRuntimeContext().getMetricGroup().gauge("rightBundleSize", rightBundleSizeGauge);
    }

    @Override
    public void processElement1(StreamRecord<RowData> input) throws Exception {
        RowData joinKey = (RowData) getCurrentKey();
        RowData uniqueKey = leftInputSideSpec.getUniqueKeySelector().getKey(input.getValue());
        leftBundleSize = leftBuffer.addRecord(joinKey, uniqueKey, input.getValue());

        coBundleTrigger.onBufferSize(leftBundleSize + rightBundleSize);
    }

    @Override
    public void processElement2(StreamRecord<RowData> input) throws Exception {
        RowData joinKey = (RowData) getCurrentKey();
        RowData uniqueKey = rightInputSideSpec.getUniqueKeySelector().getKey(input.getValue());
        rightBundleSize = rightBuffer.addRecord(joinKey, uniqueKey, input.getValue());

        coBundleTrigger.onBufferSize(leftBundleSize + rightBundleSize);
    }

    @Override
    public void processWatermark1(Watermark mark) throws Exception {
        finishBundle();
        super.processWatermark1(mark);
    }

    @Override
    public void processWatermark2(Watermark mark) throws Exception {
        finishBundle();
        super.processWatermark2(mark);
    }

    @Override
    public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
        finishBundle();
    }

    @Override
    public void finish() throws Exception {
        finishBundle();
        super.finish();
    }

    @Override
    public void finishBundle() throws Exception {
        if (!leftBuffer.isEmpty() || !rightBuffer.isEmpty()) {
            this.processBundles(leftBuffer, rightBuffer);

            leftBuffer.clear();
            rightBuffer.clear();

            // update metrics value
            leftBundleSizeGauge.update(leftBundleSize);
            rightBundleSizeGauge.update(rightBundleSize);

            leftBundleSize = 0;
            rightBundleSize = 0;
        }
        coBundleTrigger.reset();
    }

    protected abstract ReduceStats processBundles(
            MiniBatchBuffer leftBuffer, MiniBatchBuffer rightBuffer) throws Exception;

    protected int processSingleSideBundles(
            MiniBatchBuffer inputBuffer,
            JoinRecordStateView inputSideStateView,
            JoinRecordStateView otherSideStateView,
            boolean inputIsLeft)
            throws Exception {
        for (Map.Entry<RowData, List<RowData>> entry : inputBuffer.getMapRecords().entrySet()) {
            // set state key first
            setCurrentKey(entry.getKey());
            for (RowData rowData : entry.getValue()) {
                processElement(rowData, inputSideStateView, otherSideStateView, inputIsLeft);
            }
        }
        // retain this reduce size parameter for next step optimization
        return 0;
    }

    public static MiniBatchStreamingJoinOperator newMiniBatchStreamJoinOperator(
            FlinkJoinType joinType,
            InternalTypeInfo<RowData> leftType,
            InternalTypeInfo<RowData> rightType,
            GeneratedJoinCondition generatedJoinCondition,
            JoinInputSideSpec leftInputSideSpec,
            JoinInputSideSpec rightInputSideSpec,
            boolean leftIsOuter,
            boolean rightIsOuter,
            boolean[] filterNullKeys,
            long stateRetentionTime,
            CoBundleTrigger<RowData, RowData> coBundleTrigger) {
        MiniBatchStreamingJoinParameter parameter =
                new MiniBatchStreamingJoinParameter(
                        leftType,
                        rightType,
                        generatedJoinCondition,
                        leftInputSideSpec,
                        rightInputSideSpec,
                        leftIsOuter,
                        rightIsOuter,
                        filterNullKeys,
                        stateRetentionTime,
                        coBundleTrigger);
        switch (joinType) {
            case INNER:
                return new MiniBatchInnerJoinStreamOperator(parameter);
            case LEFT:
                return new MiniBatchLeftOuterJoinStreamOperator(parameter);
            case RIGHT:
                return new MiniBatchRightOuterJoinStreamOperator(parameter);
            case FULL:
                return new MiniBatchFullOuterJoinStreamOperator(parameter);
            default:
                throw new UnsupportedOperationException("Unsupported join type: " + joinType);
        }
    }

    static class MiniBatchStreamingJoinParameter implements Serializable {
        InternalTypeInfo<RowData> leftType;
        InternalTypeInfo<RowData> rightType;
        GeneratedJoinCondition generatedJoinCondition;
        JoinInputSideSpec leftInputSideSpec;
        JoinInputSideSpec rightInputSideSpec;
        boolean leftIsOuter;
        boolean rightIsOuter;
        boolean[] filterNullKeys;
        long stateRetentionTime;
        CoBundleTrigger<RowData, RowData> coBundleTrigger;

        MiniBatchStreamingJoinParameter(
                InternalTypeInfo<RowData> leftType,
                InternalTypeInfo<RowData> rightType,
                GeneratedJoinCondition generatedJoinCondition,
                JoinInputSideSpec leftInputSideSpec,
                JoinInputSideSpec rightInputSideSpec,
                boolean leftIsOuter,
                boolean rightIsOuter,
                boolean[] filterNullKeys,
                long stateRetentionTime,
                CoBundleTrigger<RowData, RowData> coBundleTrigger) {
            this.leftType = leftType;
            this.rightType = rightType;
            this.generatedJoinCondition = generatedJoinCondition;
            this.leftInputSideSpec = leftInputSideSpec;
            this.rightInputSideSpec = rightInputSideSpec;
            this.leftIsOuter = leftIsOuter;
            this.rightIsOuter = rightIsOuter;
            this.filterNullKeys = filterNullKeys;
            this.stateRetentionTime = stateRetentionTime;
            this.coBundleTrigger = coBundleTrigger;
        }
    }

    /** Inner MiniBatch Join operator. */
    private static class MiniBatchInnerJoinStreamOperator extends MiniBatchStreamingJoinOperator {

        public MiniBatchInnerJoinStreamOperator(MiniBatchStreamingJoinParameter parameter) {
            super(parameter);
        }

        @Override
        protected ReduceStats processBundles(
                MiniBatchBuffer leftBuffer, MiniBatchBuffer rightBuffer) throws Exception {
            // process right
            int rightBundleReduceSize =
                    this.processSingleSideBundles(
                            rightBuffer, rightRecordStateView, leftRecordStateView, false);
            // process left
            int leftBundleReduceSize =
                    this.processSingleSideBundles(
                            leftBuffer, leftRecordStateView, rightRecordStateView, true);
            return ReduceStats.of(leftBundleReduceSize, rightBundleReduceSize);
        }
    }

    /** MiniBatch Left outer join operator. */
    private static class MiniBatchLeftOuterJoinStreamOperator
            extends MiniBatchStreamingJoinOperator {

        public MiniBatchLeftOuterJoinStreamOperator(MiniBatchStreamingJoinParameter parameter) {
            super(parameter);
        }

        @Override
        protected ReduceStats processBundles(
                MiniBatchBuffer leftBuffer, MiniBatchBuffer rightBuffer) throws Exception {
            // more efficient to process right first for left out join, i.e, some retractions can be
            // avoided
            // process right
            int rightBundleReduceSize =
                    this.processSingleSideBundles(
                            rightBuffer, rightRecordStateView, leftRecordStateView, false);
            // process left
            int leftBundleReduceSize =
                    this.processSingleSideBundles(
                            leftBuffer, leftRecordStateView, rightRecordStateView, true);
            return ReduceStats.of(leftBundleReduceSize, rightBundleReduceSize);
        }
    }

    /** MiniBatch Right outer join operator. */
    private static class MiniBatchRightOuterJoinStreamOperator
            extends MiniBatchStreamingJoinOperator {

        public MiniBatchRightOuterJoinStreamOperator(MiniBatchStreamingJoinParameter parameter) {
            super(parameter);
        }

        @Override
        protected ReduceStats processBundles(
                MiniBatchBuffer leftBuffer, MiniBatchBuffer rightBuffer) throws Exception {

            // more efficient to process left first for right out join, i.e, some retractions can be
            // avoided
            // process left
            int leftBundleReduceSize =
                    this.processSingleSideBundles(
                            leftBuffer, leftRecordStateView, rightRecordStateView, true);

            // process right
            int rightBundleReduceSize =
                    this.processSingleSideBundles(
                            rightBuffer, rightRecordStateView, leftRecordStateView, false);
            return ReduceStats.of(leftBundleReduceSize, rightBundleReduceSize);
        }
    }

    /** MiniBatch Full outer Join operator. */
    private static class MiniBatchFullOuterJoinStreamOperator
            extends MiniBatchStreamingJoinOperator {

        public MiniBatchFullOuterJoinStreamOperator(MiniBatchStreamingJoinParameter parameter) {
            super(parameter);
        }

        @Override
        protected ReduceStats processBundles(
                MiniBatchBuffer leftBuffer, MiniBatchBuffer rightBuffer) throws Exception {
            // process right
            int rightBundleReduceSize =
                    this.processSingleSideBundles(
                            rightBuffer, rightRecordStateView, leftRecordStateView, false);
            // process left
            int leftBundleReduceSize =
                    this.processSingleSideBundles(
                            leftBuffer, leftRecordStateView, rightRecordStateView, true);
            return ReduceStats.of(leftBundleReduceSize, rightBundleReduceSize);
        }
    }

    /** Record the left and right side reduced size. */
    private static class ReduceStats {
        final int leftBundleReduceSize;
        final int rightBundleReduceSize;

        private ReduceStats(int leftBundleReduceSize, int rightBundleReduceSize) {
            this.leftBundleReduceSize = leftBundleReduceSize;
            this.rightBundleReduceSize = rightBundleReduceSize;
        }

        static ReduceStats of(int leftBundleReduceSize, int rightBundleReduceSize) {
            return new ReduceStats(leftBundleReduceSize, rightBundleReduceSize);
        }
    }
}
