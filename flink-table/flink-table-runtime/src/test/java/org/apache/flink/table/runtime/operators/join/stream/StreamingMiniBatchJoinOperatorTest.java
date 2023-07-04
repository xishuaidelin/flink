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

import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.bundle.trigger.CountCoBundleTrigger;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.operators.join.stream.state.JoinInputSideSpec;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.apache.flink.table.runtime.util.StreamRecordUtils.deleteRecord;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.insertRecord;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.rowOfKind;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.updateAfterRecord;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.updateBeforeRecord;

/** Operator test. */
public final class StreamingMiniBatchJoinOperatorTest
        extends StreamingMiniBatchJoinOperatorTestbase {

    private static RowDataKeySelector leftUniqueKeySelector;
    private static RowDataKeySelector rightUniqueKeySelector;

    @Override
    protected MiniBatchStreamingJoinOperator createJoinOperator(TestInfo testInfo) {
        RowDataKeySelector[] keySelectors0 = UK_SELECTOR_EXTRACTOR.apply(testInfo.getDisplayName());
        leftUniqueKeySelector = keySelectors0[0];
        rightUniqueKeySelector = keySelectors0[1];
        JoinInputSideSpec[] inputSideSpecs = INPUT_SPEC_EXTRACTOR.apply(testInfo.getDisplayName());
        Boolean[] isOuter = JOIN_TYPE_EXTRACTOR.apply(testInfo.getDisplayName());
        FlinkJoinType joinType = FLINK_JOIN_TYPE_EXTRACTOR.apply(testInfo.getDisplayName());
        int batchSize = MINIBATCH_SIZE_EXTRACTOR.apply(testInfo.getTags());
        Long[] ttl = STATE_RETENTION_TIME_EXTRACTOR.apply(testInfo.getTags());

        return MiniBatchStreamingJoinOperator.newMiniBatchStreamJoinOperator(
                joinType,
                LEFT_TYPE_INFO,
                RIGHT_TYPE_INFO,
                joinCondition,
                inputSideSpecs[0],
                inputSideSpecs[1],
                isOuter[0],
                isOuter[1],
                new boolean[] {true},
                ttl[0],
                ttl[0],
                new CountCoBundleTrigger<>(batchSize));
    }

    @Override
    protected RowType getOutputType() {
        return RowType.of(
                Stream.concat(
                                LEFT_TYPE_INFO.toRowType().getChildren().stream(),
                                RIGHT_TYPE_INFO.toRowType().getChildren().stream())
                        .toArray(LogicalType[]::new),
                Stream.concat(
                                LEFT_TYPE_INFO.toRowType().getFieldNames().stream(),
                                RIGHT_TYPE_INFO.toRowType().getFieldNames().stream())
                        .toArray(String[]::new));
    }

    @Tag("miniBatchSize=4")
    @Test
    public void testInnerJoinJkUkNoFold() throws Exception {
        testHarness.setStateTtlProcessingTime(1);
        // basic test for that the mini-batch process could be triggerred normally
        testHarness.processElement1(
                insertRecord("Ord#1", "LineOrd#1", "1 Bellevue Drive, Pottstown, PA 19464"));
        assertor.shouldEmitNothing(testHarness);

        testHarness.processElement1(
                insertRecord("Ord#1", "LineOrd#2", "2 Bellevue Drive, Pottstown, PA 19464"));
        assertor.shouldEmitNothing(testHarness);

        testHarness.processElement2(insertRecord("Ord#Y", "LineOrd#1", "TRUCK"));
        assertor.shouldEmitNothing(testHarness);

        // exactly reach to the mini-batch size
        testHarness.processElement2(insertRecord("Ord#X", "LineOrd#2", "AIR"));
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.INSERT,
                        "Ord#1",
                        "LineOrd#1",
                        "1 Bellevue Drive, Pottstown, PA 19464",
                        "Ord#Y",
                        "LineOrd#1",
                        "TRUCK"),
                rowOfKind(
                        RowKind.INSERT,
                        "Ord#1",
                        "LineOrd#2",
                        "2 Bellevue Drive, Pottstown, PA 19464",
                        "Ord#X",
                        "LineOrd#2",
                        "AIR"));
    }

    @Tag("miniBatchSize=18")
    @Test
    public void testInnerJoinJkUkWithinBatch() throws Exception {
        // left fold  || right fold
        // +I +U / +U +U / +U -D ||  +I -D / +U -U / +I -U
        testHarness.processElement1(
                insertRecord("Ord#1", "LineOrd#1", "3 Bellevue Drive, Pottstown, PA 19464"));
        testHarness.processElement1(
                updateAfterRecord("Ord#2", "LineOrd#1", "4 Bellevue Drive, Pottstown, PB 19464"));
        testHarness.processElement2(insertRecord("Ord#X", "LineOrd#7", "RAILWAY"));
        testHarness.processElement1(
                updateAfterRecord("Ord#5", "LineOrd#5", "7 Bellevue Drive, Pottstown, PE 19464"));
        testHarness.processElement1(
                updateAfterRecord(
                        "Ord#x5", "LineOrd#5", "x3 Bellevue Drive, Pottstown, PAxx 19464"));
        testHarness.processElement1(
                insertRecord("Ord#i", "LineOrd#6", "i6 Bellevue Drive, Pottstown, Pi 19464"));
        testHarness.processElement2(deleteRecord("Ord#X", "LineOrd#7", "RAILWAY"));
        testHarness.processElement1(
                updateAfterRecord(
                        "Ord#3", "LineOrd#x3", "x5 Bellevue Drive, Pottstown, PCxx 19464"));
        testHarness.processElement1(
                deleteRecord("Ord#3", "LineOrd#x3", "14y0 Bellevue Drive, Pottstown, PJyy 19464"));
        testHarness.processElement2(updateAfterRecord("Ord#X", "LineOrd#7", "AIR"));
        testHarness.processElement1(
                insertRecord("Ord#4", "LineOrd#4", "6 Bellevue Drive, Pottstown, PD 19464"));
        assertor.shouldEmitNothing(testHarness);
        testHarness.processElement2(updateBeforeRecord("Ord#X", "LineOrd#7", "AIR"));
        testHarness.processElement2(insertRecord("Ord#X", "LineOrd#2", "AIR"));
        testHarness.processElement2(updateBeforeRecord("Ord#X", "LineOrd#2", "AIR"));
        // right state is empty
        testHarness.processElement2(insertRecord("Ord#X", "LineOrd#1", "AIR"));
        testHarness.processElement2(updateBeforeRecord("Ord#Y", "LineOrd#5", "TRUCK"));
        testHarness.processElement2(insertRecord("Ord#Y", "LineOrd#6", "RAILWAY"));
        testHarness.processElement2(
                updateBeforeRecord("Ord#Z", "LineOrd#4", "RAILWAY")); // no effect to state
        // left state  |  right state
        // "Ord#2", "LineOrd#1", "4 Bellevue Drive, Pottstown, PB 19464"     |  "Ord#X",
        // "LineOrd#1", "AIR"
        // "Ord#x5", "LineOrd#5", "x3 Bellevue Drive, Pottstown, PAxx 19464" |  "Ord#Y",
        // "LineOrd#6", "RAILWAY"
        // "Ord#i", "LineOrd#6", "i6 Bellevue Drive, Pottstown, Pi 19464"
        // "Ord#4", "LineOrd#4", "6 Bellevue Drive, Pottstown, PD 19464"
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.INSERT,
                        "Ord#i",
                        "LineOrd#6",
                        "i6 Bellevue Drive, Pottstown, Pi 19464",
                        "Ord#Y",
                        "LineOrd#6",
                        "RAILWAY"),
                rowOfKind(
                        RowKind.UPDATE_AFTER,
                        "Ord#2",
                        "LineOrd#1",
                        "4 Bellevue Drive, Pottstown, PB 19464",
                        "Ord#X",
                        "LineOrd#1",
                        "AIR"));
    }

    @Tag("miniBatchSize=10")
    @Test
    public void testInnerJoinJkUkWithBatches() throws Exception {
        testHarness.processElement1(
                insertRecord("Ord#1", "LineOrd#1", "3 Bellevue Drive, Pottstown, PA 19464"));
        testHarness.processElement1(
                insertRecord("Ord#3", "LineOrd#3", "5 Bellevue Drive, Pottstown, PC 19464"));
        testHarness.processElement2(insertRecord("Ord#Y", "LineOrd#4", "TRUCK"));
        assertor.shouldEmitNothing(testHarness);
        testHarness.processElement1(
                insertRecord("Ord#4", "LineOrd#4", "6 Bellevue Drive, Pottstown, PD 19464"));
        testHarness.processElement1(
                insertRecord("Ord#5", "LineOrd#5", "7 Bellevue Drive, Pottstown, PE 19464"));
        testHarness.processElement1(
                updateBeforeRecord("Ord#5", "LineOrd#5", "7 Bellevue Drive, Pottstown, PE 19464"));
        testHarness.processElement1(
                updateBeforeRecord("Ord#9", "LineOrd#9", "11 Bellevue Drive, Pottstown, PI 19464"));
        assertor.shouldEmitNothing(testHarness);
        testHarness.processElement2(insertRecord("Ord#X", "LineOrd#9", "AIR"));
        testHarness.processElement2(updateAfterRecord("Ord#xyz", "LineOrd#1", "SHIP"));
        testHarness.processElement2(deleteRecord("Ord#Y", "LineOrd#4", "TRUCK"));
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.UPDATE_BEFORE,
                        "Ord#9",
                        "LineOrd#9",
                        "11 Bellevue Drive, Pottstown, PI 19464",
                        "Ord#X",
                        "LineOrd#9",
                        "AIR"),
                rowOfKind(
                        RowKind.INSERT,
                        "Ord#1",
                        "LineOrd#1",
                        "3 Bellevue Drive, Pottstown, PA 19464",
                        "Ord#xyz",
                        "LineOrd#1",
                        "SHIP"));
        // second join:
        // 1.left stream state(last batch defined) join new input from right stream.
        // 2.right stream state(current and last batch defined) join new input from left stream.

        testHarness.processElement1(
                updateAfterRecord(
                        "Ord#adjust", "LineOrd#4", "14 Bellevue Drive, Pottstown, PJ 19464"));
        testHarness.processElement1(
                updateAfterRecord("Ord#18", "LineOrd#9", "22 Bellevue Drive, Pottstown, PK 19464"));
        testHarness.processElement2(deleteRecord("Ord#X", "LineOrd#x3", "AIR"));
        testHarness.processElement2(updateBeforeRecord("Ord#xyz", "LineOrd#1", "SHIP"));
        testHarness.processElement2(insertRecord("Ord#Y", "LineOrd#4", "TRUCK"));
        testHarness.processElement1(
                updateAfterRecord("Ord#14", "LineOrd#4", "18 Bellevue Drive, Pottstown, PL 19464"));
        testHarness.processElement1(
                deleteRecord("Ord#3", "LineOrd#x3", "x5 Bellevue Drive, Pottstown, PCxx 19464"));
        testHarness.processElement1(
                insertRecord("Ord#3", "LineOrd#x3", "x5 Bellevue Drive, Pottstown, PCxx 19464"));
        testHarness.processElement1(
                insertRecord(
                        "Ord#10", "LineOrd#100y", "14y0 Bellevue Drive, Pottstown, PJyy 19464"));
        assertor.shouldEmitNothing(testHarness);

        testHarness.processElement2(updateAfterRecord("Ord#101", "LineOrd#x3", "AIR"));

        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.INSERT,
                        "Ord#4",
                        "LineOrd#4",
                        "6 Bellevue Drive, Pottstown, PD 19464",
                        "Ord#Y",
                        "LineOrd#4",
                        "TRUCK"),
                rowOfKind(
                        RowKind.UPDATE_BEFORE,
                        "Ord#1",
                        "LineOrd#1",
                        "3 Bellevue Drive, Pottstown, PA 19464",
                        "Ord#xyz",
                        "LineOrd#1",
                        "SHIP"),
                rowOfKind(
                        RowKind.DELETE,
                        "Ord#3",
                        "LineOrd#x3",
                        "x5 Bellevue Drive, Pottstown, PCxx 19464",
                        "Ord#101",
                        "LineOrd#x3",
                        "AIR"),
                rowOfKind(
                        RowKind.INSERT,
                        "Ord#3",
                        "LineOrd#x3",
                        "x5 Bellevue Drive, Pottstown, PCxx 19464",
                        "Ord#101",
                        "LineOrd#x3",
                        "AIR"),
                rowOfKind(
                        RowKind.UPDATE_AFTER,
                        "Ord#18",
                        "LineOrd#9",
                        "22 Bellevue Drive, Pottstown, PK 19464",
                        "Ord#X",
                        "LineOrd#9",
                        "AIR"),
                rowOfKind(
                        RowKind.UPDATE_AFTER,
                        "Ord#14",
                        "LineOrd#4",
                        "18 Bellevue Drive, Pottstown, PL 19464",
                        "Ord#Y",
                        "LineOrd#4",
                        "TRUCK"));
    }

    @Tag("miniBatchSize=13")
    @Test
    public void testInnerJoinWithHasUkWithinBatch() throws Exception {
        // +I +U / +I -U / +I -D / +U -D /+U +U
        List<StreamRecord<RowData>> records =
                Arrays.asList(
                        insertRecord(
                                "Ord#1", "LineOrd#1", "3 Bellevue Drive, Pottstown, PA 19464"), // 2
                        insertRecord("Ord#2", "LineOrd#2", "4 Bellevue Drive, Pottstown, PB 19464"),
                        insertRecord(
                                "Ord#3",
                                "LineOrd#10",
                                "5 Bellevue Drive, Pottstown, PC 19464"), // 1
                        updateAfterRecord(
                                "Ord#3",
                                "LineOrd#10",
                                "xxx Bellevue Drive, Pottstown, PJ 19464"), // 1
                        updateAfterRecord(
                                "Ord#5", "LineOrd#5", "7 Bellevue Drive, Pottstown, PE 19464"),
                        insertRecord(
                                "Ord#6", "LineOrd#5", "8 Bellevue Drive, Pottstown, PF 19464"), // 3
                        updateBeforeRecord(
                                "Ord#1", "LineOrd#1", "3 Bellevue Drive, Pottstown, PA 19464"), // 2
                        deleteRecord(
                                "Ord#6", "LineOrd#5", "7 Bellevue Drive, Pottstown, PE 19464"), // 3
                        updateBeforeRecord(
                                "Ord#12",
                                "LineOrd#4",
                                "6 Bellevue Drive, Pottstown, PD 19464"), // no effect
                        updateAfterRecord(
                                "Ord#9", "LineOrd#3", "5 Bellevue Drive, Pottstown, PC 19464"), // 4
                        deleteRecord(
                                "Ord#9",
                                "LineOrd#3",
                                "5 Bellevue Drive, Pottstown, PC 19464")); // 4
        for (StreamRecord<RowData> row : records) {
            testHarness.processElement1(row);
        }
        // left state
        // "Ord#2", "LineOrd#2", "4 Bellevue Drive, Pottstown, PB 19464"
        // "Ord#3", "LineOrd#10", "xxx Bellevue Drive, Pottstown, PJ 19464"
        // "Ord#5", "LineOrd#5", "7 Bellevue Drive, Pottstown, PE 19464"
        assertor.shouldEmitNothing(testHarness);
        records =
                Arrays.asList(
                        insertRecord("Ord#5", "LineOrd#2", "SHIP"),
                        updateAfterRecord("Ord#6", "LineOrd#5", "AIR"));
        for (StreamRecord<RowData> row : records) {
            testHarness.processElement2(row);
        }
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.UPDATE_AFTER,
                        "Ord#5",
                        "LineOrd#5",
                        "7 Bellevue Drive, Pottstown, PE 19464",
                        "Ord#6",
                        "LineOrd#5",
                        "AIR"),
                rowOfKind(
                        RowKind.INSERT,
                        "Ord#2",
                        "LineOrd#2",
                        "4 Bellevue Drive, Pottstown, PB 19464",
                        "Ord#5",
                        "LineOrd#2",
                        "SHIP"));
    }

    @Tag("miniBatchSize=8")
    @Test
    public void testInnerJoinWithHasUkWithBatches() throws Exception {
        // fold +I/+U +U (same and different jks)
        testHarness.processElement1(
                updateBeforeRecord("Ord#1", "LineOrd#1", "3 Bellevue Drive, Pottstown, PA 19464"));
        testHarness.processElement1(
                updateAfterRecord("Ord#2", "LineOrd#2", "4 Bellevue Drive, Pottstown, PB 19464"));
        testHarness.processElement2(insertRecord("Ord#5", "LineOrd#5", "SHIP"));
        testHarness.processElement1(
                insertRecord("Ord#4", "LineOrd#4", "5 Bellevue Drive, Pottstown, PC 19464"));
        testHarness.processElement1(
                updateAfterRecord("Ord#4", "LineOrd#4", "6 Bellevue Drive, Pottstown, PD 19464"));
        testHarness.processElement2(updateAfterRecord("Ord#22", "LineOrd#4", "SHIP"));
        testHarness.processElement2(insertRecord("Ord#23", "LineOrd#10", "AIR"));
        assertor.shouldEmitNothing(testHarness);
        testHarness.processElement1(
                updateAfterRecord("Ord#4", "LineOrd#4", "xxx Bellevue Drive, Pottstown, PJ 19464"));
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.UPDATE_AFTER,
                        "Ord#4",
                        "LineOrd#4",
                        "xxx Bellevue Drive, Pottstown, PJ 19464",
                        "Ord#22",
                        "LineOrd#4",
                        "SHIP"));

        // fold +I/+U -U/D (same and different jks)
        testHarness.processElement1(
                insertRecord("Ord#5", "LineOrd#5", "7 Bellevue Drive, Pottstown, PE 19464"));
        testHarness.processElement1(
                updateBeforeRecord("Ord#6", "LineOrd#6", "8 Bellevue Drive, Pottstown, PF 19464"));
        testHarness.processElement2(insertRecord("Ord#21", "LineOrd#5", "RAILWAY"));
        testHarness.processElement2(insertRecord("Ord#1", "LineOrd#5", "TRUCK"));
        testHarness.processElement1(
                updateAfterRecord("Ord#6", "LineOrd#6", "8 Bellevue Drive, Pottstown, PF 19464"));
        testHarness.processElement1(
                deleteRecord("Ord#2", "LineOrd#2", "4 Bellevue Drive, Pottstown, PB 19464"));
        testHarness.processElement2(updateBeforeRecord("Ord#5", "LineOrd#5", "SHIP"));
        assertor.shouldEmitNothing(testHarness);
        testHarness.processElement2(updateBeforeRecord("Ord#22", "LineOrd#6", "AIR"));

        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.INSERT,
                        "Ord#5",
                        "LineOrd#5",
                        "7 Bellevue Drive, Pottstown, PE 19464",
                        "Ord#21",
                        "LineOrd#5",
                        "RAILWAY"),
                rowOfKind(
                        RowKind.INSERT,
                        "Ord#5",
                        "LineOrd#5",
                        "7 Bellevue Drive, Pottstown, PE 19464",
                        "Ord#1",
                        "LineOrd#5",
                        "TRUCK"));
    }

    @Tag("miniBatchSize=18")
    @Test
    public void testInnerJoinWithNoUkWithinBatch() throws Exception {
        // +I -U / +I -D / -U +U / -D +I
        List<StreamRecord<RowData>> records =
                Arrays.asList(
                        insertRecord("Ord#1", "LineOrd#1", "3 Bellevue Drive, Pottstown, PA 19464"),
                        insertRecord("Ord#1", "LineOrd#2", "4 Bellevue Drive, Pottstown, PB 19464"),
                        insertRecord(
                                "Ord#1",
                                "LineOrd#2",
                                "4 Bellevue Drive, Pottstown, PB 19464"), // 2x
                        deleteRecord("Ord#6", "LineOrd#6", "8 Bellevue Drive, Pottstown, PF 19464"),
                        insertRecord("Ord#3", "LineOrd#1", "3 Bellevue Drive, Pottstown, PA 19464"),
                        insertRecord(
                                "Ord#3", "LineOrd#3", "5 Bellevue Drive, Pottstown, PD 19464"), // x
                        updateBeforeRecord(
                                "Ord#3", "LineOrd#3", "5 Bellevue Drive, Pottstown, PD 19464"), // x
                        updateBeforeRecord(
                                "Ord#9",
                                "LineOrd#9",
                                "11 Bellevue Drive, Pottstown, PI 19464"), // 3x
                        updateAfterRecord(
                                "Ord#10", "LineOrd#10", "14 Bellevue Drive, Pottstown, PJ 19464"),
                        updateAfterRecord(
                                "Ord#18", "LineOrd#18", "22 Bellevue Drive, Pottstown, PK 19464"),
                        deleteRecord(
                                "Ord#1",
                                "LineOrd#2",
                                "4 Bellevue Drive, Pottstown, PB 19464"), // 2x
                        updateAfterRecord(
                                "Ord#9",
                                "LineOrd#9",
                                "11 Bellevue Drive, Pottstown, PI 19464"), // 3x
                        deleteRecord(
                                "Ord#6",
                                "LineOrd#6",
                                "8 Bellevue Drive, Pottstown, PF 19464"), // 4x
                        insertRecord(
                                "Ord#6", "LineOrd#6", "8 Bellevue Drive, Pottstown, PF 19464") // 4x
                        );
        for (StreamRecord<RowData> row : records) {
            testHarness.processElement2(row);
        }
        records =
                Arrays.asList(
                        insertRecord("Ord#1", "LineOrd#1", "AIR"),
                        updateAfterRecord("Ord#1", "LineOrd#2", "SHIP"),
                        insertRecord("Ord#1", "LineOrd#2", "TRUCK"),
                        deleteRecord("Ord#6", "LineOrd#6", "RAILWAY"));

        for (StreamRecord<RowData> row : records) {
            testHarness.processElement1(row);
        }
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.INSERT,
                        "Ord#1",
                        "LineOrd#1",
                        "AIR",
                        "Ord#3",
                        "LineOrd#1",
                        "3 Bellevue Drive, Pottstown, PA 19464"),
                rowOfKind(
                        RowKind.INSERT,
                        "Ord#1",
                        "LineOrd#1",
                        "AIR",
                        "Ord#1",
                        "LineOrd#1",
                        "3 Bellevue Drive, Pottstown, PA 19464"),
                rowOfKind(
                        RowKind.UPDATE_AFTER,
                        "Ord#1",
                        "LineOrd#2",
                        "SHIP",
                        "Ord#1",
                        "LineOrd#2",
                        "4 Bellevue Drive, Pottstown, PB 19464"),
                rowOfKind(
                        RowKind.INSERT,
                        "Ord#1",
                        "LineOrd#2",
                        "TRUCK",
                        "Ord#1",
                        "LineOrd#2",
                        "4 Bellevue Drive, Pottstown, PB 19464"));
    }

    @Tag("miniBatchSize=6")
    @Test
    public void testInnerJoinWithNoUkWithBatches() throws Exception {
        // completely duplicate records
        testHarness.processElement1(
                insertRecord("Ord#1", "LineOrd#2", "4 Bellevue Drive, Pottstown, PB 19464"));
        testHarness.processElement2(insertRecord("Ord#1", "LineOrd#1", "AIR"));
        testHarness.processElement1(
                insertRecord("Ord#1", "LineOrd#2", "4 Bellevue Drive, Pottstown, PB 19464"));
        testHarness.processElement1(
                deleteRecord("Ord#6", "LineOrd#6", "8 Bellevue Drive, Pottstown, PF 19464"));
        testHarness.processElement2(updateAfterRecord("Ord#1", "LineOrd#2", "SHIP"));
        assertor.shouldEmitNothing(testHarness);
        testHarness.processElement1(
                insertRecord("Ord#3", "LineOrd#3", "5 Bellevue Drive, Pottstown, PD 19464"));
        // left state   |    right state
        // "Ord#1", "LineOrd#2", "4 Bellevue Drive, Pottstown, PB 19464"  |  "Ord#1", "LineOrd#1",
        // "AIR"
        // "Ord#1", "LineOrd#2", "4 Bellevue Drive, Pottstown, PB 19464"  |  "Ord#1", "LineOrd#2",
        // "SHIP"
        // "Ord#3", "LineOrd#3", "5 Bellevue Drive, Pottstown, PD 19464"  |
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.INSERT,
                        "Ord#1",
                        "LineOrd#2",
                        "4 Bellevue Drive, Pottstown, PB 19464",
                        "Ord#1",
                        "LineOrd#2",
                        "SHIP"),
                rowOfKind(
                        RowKind.INSERT,
                        "Ord#1",
                        "LineOrd#2",
                        "4 Bellevue Drive, Pottstown, PB 19464",
                        "Ord#1",
                        "LineOrd#2",
                        "SHIP"));
        testHarness.processElement1(
                updateBeforeRecord("Ord#1", "LineOrd#2", "4 Bellevue Drive, Pottstown, PB 19464"));
        testHarness.processElement1(
                updateAfterRecord("Ord#1", "LineOrd#2", "4 Bellevue Drive, Pottstown, PB 19464"));
        testHarness.processElement2(insertRecord("Ord#1", "LineOrd#3", "AIR"));
        testHarness.processElement1(
                deleteRecord("Ord#6", "LineOrd#1", "8 Bellevue Drive, Pottstown, PF 19464"));
        testHarness.processElement2(deleteRecord("Ord#1", "LineOrd#2", "SHIP"));
        // right state
        // "Ord#1", "LineOrd#1", "AIR"
        // "Ord#1", "LineOrd#3", "AIR"
        testHarness.processElement1(
                insertRecord("Ord#1", "LineOrd#2", "4 Bellevue Drive, Pottstown, PB 19464"));
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.INSERT,
                        "Ord#3",
                        "LineOrd#3",
                        "5 Bellevue Drive, Pottstown, PD 19464",
                        "Ord#1",
                        "LineOrd#3",
                        "AIR"),
                rowOfKind(
                        RowKind.DELETE,
                        "Ord#1",
                        "LineOrd#2",
                        "4 Bellevue Drive, Pottstown, PB 19464",
                        "Ord#1",
                        "LineOrd#2",
                        "SHIP"),
                rowOfKind(
                        RowKind.DELETE,
                        "Ord#1",
                        "LineOrd#2",
                        "4 Bellevue Drive, Pottstown, PB 19464",
                        "Ord#1",
                        "LineOrd#2",
                        "SHIP"),
                rowOfKind(
                        RowKind.DELETE,
                        "Ord#6",
                        "LineOrd#1",
                        "8 Bellevue Drive, Pottstown, PF 19464",
                        "Ord#1",
                        "LineOrd#1",
                        "AIR"));
    }

    /** Outer join only emits INSERT or DELETE Msg. */
    @Tag("miniBatchSize=18")
    @Test
    public void testLeftJoinWithJkUk() throws Exception {
        // left fold  || right fold
        // +I +U / +U +U / +U -D ||  +I -D / +U -U / +I -U
        testHarness.processElement1(
                insertRecord("Ord#1", "LineOrd#1", "3 Bellevue Drive, Pottstown, PA 19464"));
        testHarness.processElement1(
                updateAfterRecord("Ord#2", "LineOrd#1", "4 Bellevue Drive, Pottstown, PB 19464"));
        testHarness.processElement2(insertRecord("Ord#X", "LineOrd#7", "RAILWAY"));
        testHarness.processElement1(
                updateAfterRecord("Ord#5", "LineOrd#5", "7 Bellevue Drive, Pottstown, PE 19464"));
        testHarness.processElement1(
                updateAfterRecord(
                        "Ord#x5", "LineOrd#5", "x3 Bellevue Drive, Pottstown, PAxx 19464"));
        testHarness.processElement1(
                insertRecord("Ord#i", "LineOrd#6", "i6 Bellevue Drive, Pottstown, Pi 19464"));
        testHarness.processElement2(deleteRecord("Ord#X", "LineOrd#7", "RAILWAY"));
        testHarness.processElement1(
                updateAfterRecord(
                        "Ord#3", "LineOrd#x3", "x5 Bellevue Drive, Pottstown, PCxx 19464"));
        testHarness.processElement1(
                deleteRecord("Ord#3", "LineOrd#x3", "14y0 Bellevue Drive, Pottstown, PJyy 19464"));
        testHarness.processElement2(updateAfterRecord("Ord#X", "LineOrd#7", "AIR"));
        testHarness.processElement1(
                insertRecord("Ord#4", "LineOrd#4", "6 Bellevue Drive, Pottstown, PD 19464"));
        assertor.shouldEmitNothing(testHarness);
        testHarness.processElement2(updateBeforeRecord("Ord#X", "LineOrd#7", "AIR"));
        testHarness.processElement2(insertRecord("Ord#X", "LineOrd#2", "AIR"));
        testHarness.processElement2(updateBeforeRecord("Ord#X", "LineOrd#2", "AIR"));
        // right state is empty
        testHarness.processElement2(insertRecord("Ord#X", "LineOrd#1", "AIR"));
        testHarness.processElement2(updateBeforeRecord("Ord#Y", "LineOrd#5", "TRUCK"));
        testHarness.processElement2(insertRecord("Ord#Y", "LineOrd#6", "RAILWAY"));
        testHarness.processElement2(
                updateBeforeRecord("Ord#Z", "LineOrd#4", "RAILWAY")); // no effect to state
        // left state  |  right state
        // "Ord#2", "LineOrd#1", "4 Bellevue Drive, Pottstown, PB 19464"     |  "Ord#X",
        // "LineOrd#1", "AIR"
        // "Ord#x5", "LineOrd#5", "x3 Bellevue Drive, Pottstown, PAxx 19464" |  "Ord#Y",
        // "LineOrd#6", "RAILWAY"
        // "Ord#i", "LineOrd#6", "i6 Bellevue Drive, Pottstown, Pi 19464"
        // "Ord#4", "LineOrd#4", "6 Bellevue Drive, Pottstown, PD 19464"
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.INSERT,
                        "Ord#i",
                        "LineOrd#6",
                        "i6 Bellevue Drive, Pottstown, Pi 19464",
                        "Ord#Y",
                        "LineOrd#6",
                        "RAILWAY"),
                rowOfKind(
                        RowKind.INSERT,
                        "Ord#4",
                        "LineOrd#4",
                        "6 Bellevue Drive, Pottstown, PD 19464",
                        null,
                        null,
                        null),
                rowOfKind(
                        RowKind.INSERT,
                        "Ord#2",
                        "LineOrd#1",
                        "4 Bellevue Drive, Pottstown, PB 19464",
                        "Ord#X",
                        "LineOrd#1",
                        "AIR"),
                rowOfKind(
                        RowKind.INSERT,
                        "Ord#x5",
                        "LineOrd#5",
                        "x3 Bellevue Drive, Pottstown, PAxx 19464",
                        null,
                        null,
                        null));
    }

    /** Outer join only emits INSERT or DELETE Msg. */
    @Tag("miniBatchSize=15")
    @Test
    public void testLeftJoinWithHasUk() throws Exception {
        // +I +U / +I -U / +I -D / +U -D /+U +U
        List<StreamRecord<RowData>> records =
                Arrays.asList(
                        insertRecord(
                                "Ord#1", "LineOrd#1", "3 Bellevue Drive, Pottstown, PA 19464"), // 2
                        insertRecord("Ord#2", "LineOrd#2", "4 Bellevue Drive, Pottstown, PB 19464"),
                        insertRecord(
                                "Ord#3",
                                "LineOrd#10",
                                "5 Bellevue Drive, Pottstown, PC 19464"), // 1
                        updateAfterRecord(
                                "Ord#3",
                                "LineOrd#10",
                                "xxx Bellevue Drive, Pottstown, PJ 19464"), // 1
                        updateAfterRecord(
                                "Ord#5", "LineOrd#5", "7 Bellevue Drive, Pottstown, PE 19464"),
                        insertRecord(
                                "Ord#6", "LineOrd#5", "8 Bellevue Drive, Pottstown, PF 19464"), // 3
                        updateBeforeRecord(
                                "Ord#1", "LineOrd#1", "3 Bellevue Drive, Pottstown, PA 19464"), // 2
                        deleteRecord(
                                "Ord#6", "LineOrd#5", "7 Bellevue Drive, Pottstown, PE 19464"), // 3
                        updateAfterRecord(
                                "Ord#6", "LineOrd#7", "8 Bellevue Drive, Pottstown, PF 19464"), // 5
                        updateAfterRecord(
                                "Ord#6", "LineOrd#7", "9 Bellevue Drive, Pottstown, PF 19464"), // 5
                        updateBeforeRecord(
                                "Ord#12",
                                "LineOrd#4",
                                "6 Bellevue Drive, Pottstown, PD 19464"), // no effect
                        updateAfterRecord(
                                "Ord#9", "LineOrd#3", "5 Bellevue Drive, Pottstown, PC 19464"), // 4
                        deleteRecord(
                                "Ord#9",
                                "LineOrd#3",
                                "5 Bellevue Drive, Pottstown, PC 19464")); // 4
        for (StreamRecord<RowData> row : records) {
            testHarness.processElement1(row);
        }
        // left state
        // "Ord#2", "LineOrd#2", "4 Bellevue Drive, Pottstown, PB 19464"
        // "Ord#3", "LineOrd#10", "xxx Bellevue Drive, Pottstown, PJ 19464"
        // "Ord#5", "LineOrd#5", "7 Bellevue Drive, Pottstown, PE 19464"
        // "Ord#6", "LineOrd#7", "9 Bellevue Drive, Pottstown, PF 19464"
        assertor.shouldEmitNothing(testHarness);
        records =
                Arrays.asList(
                        insertRecord("Ord#5", "LineOrd#2", "SHIP"),
                        updateAfterRecord("Ord#6", "LineOrd#4", "AIR"));
        for (StreamRecord<RowData> row : records) {
            testHarness.processElement2(row);
        }
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.INSERT,
                        "Ord#6",
                        "LineOrd#7",
                        "9 Bellevue Drive, Pottstown, PF 19464",
                        null,
                        null,
                        null),
                rowOfKind(
                        RowKind.INSERT,
                        "Ord#3",
                        "LineOrd#10",
                        "xxx Bellevue Drive, Pottstown, PJ 19464",
                        null,
                        null,
                        null),
                rowOfKind(
                        RowKind.DELETE,
                        "Ord#12",
                        "LineOrd#4",
                        "6 Bellevue Drive, Pottstown, PD 19464",
                        "Ord#6",
                        "LineOrd#4",
                        "AIR"),
                rowOfKind(
                        RowKind.INSERT,
                        "Ord#5",
                        "LineOrd#5",
                        "7 Bellevue Drive, Pottstown, PE 19464",
                        null,
                        null,
                        null),
                rowOfKind(
                        RowKind.INSERT,
                        "Ord#2",
                        "LineOrd#2",
                        "4 Bellevue Drive, Pottstown, PB 19464",
                        "Ord#5",
                        "LineOrd#2",
                        "SHIP"));
    }

    /** Special for the pair of retract and accumulate. */
    private void testLeftJoin() throws Exception {
        List<StreamRecord<RowData>> records =
                Arrays.asList(
                        insertRecord(
                                "Ord#2", "LineOrd#2", "4 Bellevue Drive, Pottstown, PB 19464"));
        for (StreamRecord<RowData> row : records) {
            testHarness.processElement1(row);
        }
        assertor.shouldEmitNothing(testHarness);
        records =
                Arrays.asList(
                        insertRecord("Ord#2", "LineOrd#2", "SHIP"),
                        updateBeforeRecord("Ord#2", "LineOrd#2", "SHIP"),
                        updateAfterRecord("Ord#2", "LineOrd#2", "AIR"));
        for (StreamRecord<RowData> row : records) {
            testHarness.processElement2(row);
        }
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.INSERT,
                        "Ord#2",
                        "LineOrd#2",
                        "4 Bellevue Drive, Pottstown, PB 19464",
                        "Ord#2",
                        "LineOrd#2",
                        "SHIP"),
                rowOfKind(
                        RowKind.UPDATE_BEFORE,
                        "Ord#2",
                        "LineOrd#2",
                        "4 Bellevue Drive, Pottstown, PB 19464",
                        "Ord#2",
                        "LineOrd#2",
                        "SHIP"),
                rowOfKind(
                        RowKind.INSERT,
                        "Ord#2",
                        "LineOrd#2",
                        "4 Bellevue Drive, Pottstown, PB 19464",
                        "Ord#2",
                        "LineOrd#2",
                        "AIR"));
    }

    @Test
    @Tag("miniBatchSize=2")
    @DisplayName("LeftJoinHasUkRetAndAcc")
    public void test1() throws Exception {
        testLeftJoin();
    }

    @Tag("miniBatchSize=2")
    @DisplayName("LeftJoinJkUkRetAndAcc")
    @Test
    public void test3() throws Exception {
        testLeftJoin();
    }

    @Tag("miniBatchSize=15")
    @Test
    public void testLeftJoinWithNoUk() throws Exception {
        // +I -U / +I -D / -U +U / -D +I
        List<StreamRecord<RowData>> records =
                Arrays.asList(
                        insertRecord("Ord#1", "LineOrd#1", "3 Bellevue Drive, Pottstown, PA 19464"),
                        insertRecord("Ord#1", "LineOrd#2", "4 Bellevue Drive, Pottstown, PB 19464"),
                        insertRecord(
                                "Ord#1",
                                "LineOrd#2",
                                "4 Bellevue Drive, Pottstown, PB 19464"), // 2x
                        deleteRecord("Ord#6", "LineOrd#6", "8 Bellevue Drive, Pottstown, PF 19464"),
                        insertRecord(
                                "Ord#3", "LineOrd#3", "5 Bellevue Drive, Pottstown, PD 19464"), // x
                        updateBeforeRecord(
                                "Ord#3", "LineOrd#3", "5 Bellevue Drive, Pottstown, PD 19464"), // x
                        updateBeforeRecord(
                                "Ord#9",
                                "LineOrd#9",
                                "11 Bellevue Drive, Pottstown, PI 19464"), // 3x
                        updateAfterRecord(
                                "Ord#10", "LineOrd#10", "14 Bellevue Drive, Pottstown, PJ 19464"),
                        deleteRecord(
                                "Ord#1",
                                "LineOrd#2",
                                "4 Bellevue Drive, Pottstown, PB 19464"), // 2x
                        updateAfterRecord(
                                "Ord#9",
                                "LineOrd#9",
                                "11 Bellevue Drive, Pottstown, PI 19464"), // 3x
                        deleteRecord(
                                "Ord#6",
                                "LineOrd#6",
                                "8 Bellevue Drive, Pottstown, PF 19464"), // 4x
                        insertRecord(
                                "Ord#6", "LineOrd#6", "8 Bellevue Drive, Pottstown, PF 19464") // 4x
                        );

        for (StreamRecord<RowData> row : records) {
            testHarness.processElement2(row);
        }
        // right state
        // "Ord#1", "LineOrd#1", "3 Bellevue Drive, Pottstown, PA 19464"
        // "Ord#1", "LineOrd#2", "4 Bellevue Drive, Pottstown, PB 19464"
        // "Ord#10", "LineOrd#10", "14 Bellevue Drive, Pottstown, PJ 19464"
        records =
                Arrays.asList(
                        insertRecord("Ord#1", "LineOrd#1", "AIR"),
                        updateAfterRecord("Ord#1", "LineOrd#3", "SHIP"),
                        deleteRecord("Ord#6", "LineOrd#6", "RAILWAY"));

        for (StreamRecord<RowData> row : records) {
            testHarness.processElement1(row);
        }
        assertor.shouldEmit(
                testHarness,
                rowOfKind(RowKind.DELETE, "Ord#6", "LineOrd#6", "RAILWAY", null, null, null),
                rowOfKind(RowKind.INSERT, "Ord#1", "LineOrd#3", "SHIP", null, null, null),
                rowOfKind(
                        RowKind.INSERT,
                        "Ord#1",
                        "LineOrd#1",
                        "AIR",
                        "Ord#1",
                        "LineOrd#1",
                        "3 Bellevue Drive, Pottstown, PA 19464"));
    }

    private static final Function<String, Boolean[]> JOIN_TYPE_EXTRACTOR =
            (testDisplayName) -> {
                if (testDisplayName.contains("InnerJoin")) {
                    return new Boolean[] {false, false};
                } else if (testDisplayName.contains("LeftJoin")) {
                    return new Boolean[] {true, false};
                } else {
                    return new Boolean[] {false, true};
                }
            };

    private static final Function<String, FlinkJoinType> FLINK_JOIN_TYPE_EXTRACTOR =
            (testDisplayName) -> {
                if (testDisplayName.contains("InnerJoin")) {
                    return FlinkJoinType.INNER;
                } else if (testDisplayName.contains("LeftJoin")) {
                    return FlinkJoinType.LEFT;
                } else if (testDisplayName.contains("RightJoin")) {
                    return FlinkJoinType.RIGHT;
                } else {
                    return FlinkJoinType.FULL;
                }
            };

    private static final Function<String, JoinInputSideSpec[]> INPUT_SPEC_EXTRACTOR =
            (testDisplayName) -> {
                if (testDisplayName.contains("JkUk")) {
                    return new JoinInputSideSpec[] {
                        JoinInputSideSpec.withUniqueKeyContainedByJoinKey(
                                LEFT_TYPE_INFO, leftUniqueKeySelector),
                        JoinInputSideSpec.withUniqueKeyContainedByJoinKey(
                                RIGHT_TYPE_INFO, rightUniqueKeySelector)
                    };
                } else if (testDisplayName.contains("HasUk")) {
                    return new JoinInputSideSpec[] {
                        JoinInputSideSpec.withUniqueKey(LEFT_TYPE_INFO, leftUniqueKeySelector),
                        JoinInputSideSpec.withUniqueKey(RIGHT_TYPE_INFO, rightUniqueKeySelector)
                    };
                } else {
                    return new JoinInputSideSpec[] {
                        JoinInputSideSpec.withoutUniqueKey(), JoinInputSideSpec.withoutUniqueKey()
                    };
                }
            };
}
