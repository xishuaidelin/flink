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
import org.apache.flink.table.data.util.RowDataUtil;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.operators.bundle.trigger.BundleTriggerCallback;
import org.apache.flink.table.runtime.operators.bundle.trigger.CoBundleTrigger;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.operators.join.stream.bundle.BufferBundle;
import org.apache.flink.table.runtime.operators.join.stream.bundle.InputSideHasNoUniqueKeyBundle;
import org.apache.flink.table.runtime.operators.join.stream.bundle.InputSideHasUniqueKeyBundle;
import org.apache.flink.table.runtime.operators.join.stream.bundle.JoinKeyContainsUniqueKeyBundle;
import org.apache.flink.table.runtime.operators.join.stream.state.JoinInputSideSpec;
import org.apache.flink.table.runtime.operators.join.stream.state.JoinRecordStateView;
import org.apache.flink.table.runtime.operators.metrics.SimpleGauge;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/** Streaming unbounded Join base operator which support mini-batch join. */
public abstract class MiniBatchStreamingJoinOperator extends StreamingJoinOperator
        implements BundleTriggerCallback {

    private static final long serialVersionUID = -1106342589994963997L;

    private final CoBundleTrigger<RowData, RowData> coBundleTrigger;

    private transient BufferBundle leftBuffer;
    private transient BufferBundle rightBuffer;

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
                parameter.leftStateRetentionTime,
                parameter.rightStateRetentionTime);

        this.coBundleTrigger = parameter.coBundleTrigger;
    }

    @Override
    public void open() throws Exception {
        super.open();
        // must copy in mini-batch
        this.requiresCopy = true;

        this.leftBuffer = initialBuffer(leftInputSideSpec);
        this.rightBuffer = initialBuffer(rightInputSideSpec);

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
    public void processElement1(StreamRecord<RowData> element) throws Exception {
        RowData joinKey = (RowData) getCurrentKey();
        RowData uniqueKey = null;
        if (leftInputSideSpec.getUniqueKeySelector() != null) {
            uniqueKey = leftInputSideSpec.getUniqueKeySelector().getKey(element.getValue());
        }
        leftBundleSize = leftBuffer.addRecord(joinKey, uniqueKey, element.getValue());
        coBundleTrigger.onBundleSize(leftBundleSize + rightBundleSize);
    }

    @Override
    public void processElement2(StreamRecord<RowData> element) throws Exception {
        RowData joinKey = (RowData) getCurrentKey();
        RowData uniqueKey = null;
        if (rightInputSideSpec.getUniqueKeySelector() != null) {
            uniqueKey = rightInputSideSpec.getUniqueKeySelector().getKey(element.getValue());
        }
        rightBundleSize = rightBuffer.addRecord(joinKey, uniqueKey, element.getValue());
        coBundleTrigger.onBundleSize(leftBundleSize + rightBundleSize);
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

    protected abstract ReduceStats processBundles(BufferBundle leftBuffer, BufferBundle rightBuffer)
            throws Exception;

    private BufferBundle initialBuffer(JoinInputSideSpec inputSideSpec) {
        if (inputSideSpec.joinKeyContainsUniqueKey()) {
            return new JoinKeyContainsUniqueKeyBundle();
        }
        if (inputSideSpec.hasUniqueKey()) {
            return new InputSideHasUniqueKeyBundle();
        }
        return new InputSideHasNoUniqueKeyBundle();
    }

    private void processElementWithSuppress(
            Iterator<RowData> iter,
            JoinRecordStateView inputSideStateView,
            JoinRecordStateView otherSideStateView,
            boolean inputIsLeft)
            throws Exception {
        RowData pre = null; // always retractMsg if not null
        while (iter.hasNext()) {
            RowData current = iter.next();
            boolean isSuppress = false;
            if (RowDataUtil.isRetractMsg(current) && iter.hasNext()) {
                RowData next = iter.next();
                if (RowDataUtil.isAccumulateMsg(next)) {
                    isSuppress = true;
                } else {
                    // retract + retract
                    pre = next;
                }
                processElement(
                        current, inputSideStateView, otherSideStateView, inputIsLeft, isSuppress);
                if (isSuppress) {
                    processElement(
                            next, inputSideStateView, otherSideStateView, inputIsLeft, isSuppress);
                }
            } else {
                // 1. current is accumulateMsg 2. current is retractMsg and no next row
                if (pre != null) {
                    if (RowDataUtil.isAccumulateMsg(current)) {
                        isSuppress = true;
                    }
                    processElement(
                            pre, inputSideStateView, otherSideStateView, inputIsLeft, isSuppress);
                    pre = null;
                }
                processElement(
                        current, inputSideStateView, otherSideStateView, inputIsLeft, isSuppress);
            }
        }
    }

    /**
     * RetractMsg+accumulatingMsg would be optimized which would keep sending retractMsg but do not
     * deal with state.
     */
    protected int processSingleSideBundles(
            BufferBundle inputBuffer,
            JoinRecordStateView inputSideStateView,
            JoinRecordStateView otherSideStateView,
            boolean inputIsLeft)
            throws Exception {
        if (inputBuffer instanceof InputSideHasNoUniqueKeyBundle) {
            // -U/+U pair is already folded in the buffer so it is no need to go to
            // processElementByPair function
            for (Map.Entry<RowData, List<RowData>> entry : inputBuffer.getRecords().entrySet()) {
                // set state key first
                setCurrentKey(entry.getKey());
                for (RowData rowData : entry.getValue()) {
                    processElement(
                            rowData, inputSideStateView, otherSideStateView, inputIsLeft, false);
                }
            }
        } else if (inputBuffer instanceof JoinKeyContainsUniqueKeyBundle) {
            for (Map.Entry<RowData, List<RowData>> entry : inputBuffer.getRecords().entrySet()) {
                // set state key first
                setCurrentKey(entry.getKey());
                Iterator<RowData> iter = entry.getValue().iterator();
                processElementWithSuppress(
                        iter, inputSideStateView, otherSideStateView, inputIsLeft);
            }
        } else {
            for (RowData jk : inputBuffer.getJoinKeys()) {
                // set state key first
                setCurrentKey(jk);
                for (Map.Entry<RowData, List<RowData>> entry :
                        inputBuffer.getRecordsWithJoinKey(jk).entrySet()) {
                    Iterator<RowData> iter = entry.getValue().iterator();
                    processElementWithSuppress(
                            iter, inputSideStateView, otherSideStateView, inputIsLeft);
                }
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
            long leftStateRetentionTime,
            long rightStateRetentionTime,
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
                        leftStateRetentionTime,
                        rightStateRetentionTime,
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
        long leftStateRetentionTime;
        long rightStateRetentionTime;

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
                long leftStateRetentionTime,
                long rightStateRetentionTime,
                CoBundleTrigger<RowData, RowData> coBundleTrigger) {
            this.leftType = leftType;
            this.rightType = rightType;
            this.generatedJoinCondition = generatedJoinCondition;
            this.leftInputSideSpec = leftInputSideSpec;
            this.rightInputSideSpec = rightInputSideSpec;
            this.leftIsOuter = leftIsOuter;
            this.rightIsOuter = rightIsOuter;
            this.filterNullKeys = filterNullKeys;
            this.leftStateRetentionTime = leftStateRetentionTime;
            this.rightStateRetentionTime = rightStateRetentionTime;
            this.coBundleTrigger = coBundleTrigger;
        }
    }

    /** Inner MiniBatch Join operator. */
    private static class MiniBatchInnerJoinStreamOperator extends MiniBatchStreamingJoinOperator {

        public MiniBatchInnerJoinStreamOperator(MiniBatchStreamingJoinParameter parameter) {
            super(parameter);
        }

        @Override
        protected ReduceStats processBundles(BufferBundle leftBuffer, BufferBundle rightBuffer)
                throws Exception {
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
        protected ReduceStats processBundles(BufferBundle leftBuffer, BufferBundle rightBuffer)
                throws Exception {
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
        protected ReduceStats processBundles(BufferBundle leftBuffer, BufferBundle rightBuffer)
                throws Exception {

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
        protected ReduceStats processBundles(BufferBundle leftBuffer, BufferBundle rightBuffer)
                throws Exception {
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
