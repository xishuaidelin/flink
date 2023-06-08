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

    @Tag("miniBatchSize=15")
    @Test
    public void testInnerJoinJkUkWithFold() throws Exception {
        testHarness.processElement1(
                insertRecord("Ord#1", "LineOrd#1", "3 Bellevue Drive, Pottstown, PA 19464"));
        testHarness.processElement1(
                insertRecord("Ord#2", "LineOrd#2", "4 Bellevue Drive, Pottstown, PB 19464"));
        testHarness.processElement1(
                insertRecord("Ord#3", "LineOrd#3", "5 Bellevue Drive, Pottstown, PC 19464"));
        testHarness.processElement1(
                insertRecord("Ord#4", "LineOrd#4", "6 Bellevue Drive, Pottstown, PD 19464"));
        testHarness.processElement1(
                insertRecord("Ord#5", "LineOrd#5", "7 Bellevue Drive, Pottstown, PE 19464"));
        testHarness.processElement1(
                insertRecord("Ord#6", "LineOrd#6", "8 Bellevue Drive, Pottstown, PF 19464"));
        testHarness.processElement1(
                insertRecord("Ord#7", "LineOrd#7", "9 Bellevue Drive, Pottstown, PG 19464"));
        testHarness.processElement1(
                insertRecord("Ord#8", "LineOrd#8", "10 Bellevue Drive, Pottstown, PH 19464"));
        testHarness.processElement1(
                insertRecord("Ord#9", "LineOrd#9", "11 Bellevue Drive, Pottstown, PI 19464"));
        // size = 9
        testHarness.processElement1(
                updateBeforeRecord("Ord#5", "LineOrd#5", "7 Bellevue Drive, Pottstown, PE 19464"));
        // size = 8
        testHarness.processElement1(
                updateBeforeRecord("Ord#9", "LineOrd#9", "11 Bellevue Drive, Pottstown, PI 19464"));
        // size = 7
        testHarness.processElement1(
                updateBeforeRecord("Ord#7", "LineOrd#7", "9 Bellevue Drive, Pottstown, PG 19464"));
        // size = 6
        testHarness.processElement1(
                updateAfterRecord(
                        "Ord#10", "LineOrd#10", "14 Bellevue Drive, Pottstown, PJ 19464"));
        // size = 7
        testHarness.processElement1(
                updateAfterRecord(
                        "Ord#18", "LineOrd#18", "22 Bellevue Drive, Pottstown, PK 19464"));
        // size = 8
        testHarness.processElement1(
                updateAfterRecord(
                        "Ord#14", "LineOrd#14", "18 Bellevue Drive, Pottstown, PL 19464"));
        // size = 9
        testHarness.processElement1(
                deleteRecord("Ord#4", "LineOrd#4", "6 Bellevue Drive, Pottstown, PD 19464"));
        // size = 8
        testHarness.processElement1(
                deleteRecord("Ord#6", "LineOrd#6", "8 Bellevue Drive, Pottstown, PF 19464"));
        // size = 7
        testHarness.processElement1(
                updateAfterRecord(
                        "Ord#1", "LineOrd#x1", "x3 Bellevue Drive, Pottstown, PAxx 19464"));
        // size = 8
        testHarness.processElement1(
                updateAfterRecord(
                        "Ord#2", "LineOrd#x2", "x4 Bellevue Drive, Pottstown, PBxx 19464"));
        // size = 9
        testHarness.processElement1(
                updateAfterRecord(
                        "Ord#3", "LineOrd#x3", "x5 Bellevue Drive, Pottstown, PCxx 19464"));
        // size = 10
        testHarness.processElement1(
                updateAfterRecord(
                        "Ord#10", "LineOrd#100y", "14y0 Bellevue Drive, Pottstown, PJyy 19464"));
        // size = 11
        testHarness.processElement1(
                updateBeforeRecord(
                        "Ord#10", "LineOrd#100y", "14y0 Bellevue Drive, Pottstown, PJyy 19464"));
        // size = 10
        testHarness.processElement1(
                deleteRecord("Ord#3", "LineOrd#x3", "x5 Bellevue Drive, Pottstown, PCxx 19464"));
        // size = 9
        testHarness.processElement1(
                insertRecord("Ord#3", "LineOrd#x3", "x5 Bellevue Drive, Pottstown, PCxx 19464"));
        // size = 10
        testHarness.processElement1(
                insertRecord(
                        "Ord#10", "LineOrd#100y", "14y0 Bellevue Drive, Pottstown, PJyy 19464"));
        // size = 11
        testHarness.processElement1(
                updateAfterRecord("Ord#7", "LineOrd#7", "9 Bellevue Drive, Pottstown, PG 19464"));
        // size = 12
        testHarness.processElement1(
                updateAfterRecord("Ord#4", "LineOrd#4", "6 Bellevue Drive, Pottstown, PD 19464"));
        // size = 13
        assertor.shouldEmitNothing(testHarness);

        testHarness.processElement2(insertRecord("Ord#Y", "LineOrd#4", "TRUCK"));
        testHarness.processElement2(insertRecord("Ord#X", "LineOrd#7", "AIR"));

        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.UPDATE_AFTER,
                        "Ord#7",
                        "LineOrd#7",
                        "9 Bellevue Drive, Pottstown, PG 19464",
                        "Ord#X",
                        "LineOrd#7",
                        "AIR"),
                rowOfKind(
                        RowKind.UPDATE_AFTER,
                        "Ord#4",
                        "LineOrd#4",
                        "6 Bellevue Drive, Pottstown, PD 19464",
                        "Ord#Y",
                        "LineOrd#4",
                        "TRUCK"));
    }

    @Tag("miniBatchSize=10")
    @Test
    public void testInnerJoinWithHasUk() throws Exception {
        List<StreamRecord<RowData>> records =
                Arrays.asList(
                        insertRecord("Ord#1", "LineOrd#1", "3 Bellevue Drive, Pottstown, PA 19464"),
                        insertRecord("Ord#2", "LineOrd#2", "4 Bellevue Drive, Pottstown, PB 19464"),
                        insertRecord("Ord#3", "LineOrd#3", "5 Bellevue Drive, Pottstown, PC 19464"),
                        insertRecord("Ord#4", "LineOrd#4", "6 Bellevue Drive, Pottstown, PD 19464"),
                        // size = 4
                        updateAfterRecord(
                                "Ord#3", "LineOrd#10", "xxx Bellevue Drive, Pottstown, PJ 19464"),
                        // size = 4
                        insertRecord("Ord#5", "LineOrd#5", "7 Bellevue Drive, Pottstown, PE 19464"),
                        // size = 5
                        insertRecord("Ord#6", "LineOrd#6", "8 Bellevue Drive, Pottstown, PF 19464"),
                        // size = 6
                        updateAfterRecord(
                                "Ord#2", "LineOrd#2", "14 Bellevue Drive, Pottstown, PJ 19464"),
                        // size = 6
                        updateBeforeRecord(
                                "Ord#1", "LineOrd#1", "3 Bellevue Drive, Pottstown, PA 19464"),
                        // size = 5
                        deleteRecord("Ord#6", "LineOrd#5", "7 Bellevue Drive, Pottstown, PE 19464"),
                        // size = 4
                        updateAfterRecord(
                                "Ord#6", "LineOrd#6", "8 Bellevue Drive, Pottstown, PF 19464"),
                        // size = 5
                        updateBeforeRecord(
                                "Ord#12", "LineOrd#4", "6 Bellevue Drive, Pottstown, PD 19464"),
                        // size = 6
                        updateAfterRecord(
                                "Ord#xxxx",
                                "LineOrd#10",
                                "yyy Bellevue Drive, Pottstown, PJ 19464"),
                        // size = 7
                        deleteRecord("Ord#9", "LineOrd#3", "5 Bellevue Drive, Pottstown, PC 19464")
                        // size = 8
                        );
        for (StreamRecord<RowData> row : records) {
            testHarness.processElement1(row);
        }
        assertor.shouldEmitNothing(testHarness);
        records =
                Arrays.asList(
                        insertRecord("Ord#5", "LineOrd#5", "SHIP"),
                        // size = 1
                        insertRecord("Ord#6", "LineOrd#6", "AIR")
                        // size = 2
                        );
        for (StreamRecord<RowData> row : records) {
            testHarness.processElement2(row);
        }
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.UPDATE_AFTER,
                        "Ord#6",
                        "LineOrd#6",
                        "8 Bellevue Drive, Pottstown, PF 19464",
                        "Ord#6",
                        "LineOrd#6",
                        "AIR"),
                rowOfKind(
                        RowKind.INSERT,
                        "Ord#5",
                        "LineOrd#5",
                        "7 Bellevue Drive, Pottstown, PE 19464",
                        "Ord#5",
                        "LineOrd#5",
                        "SHIP"));
    }

    @Tag("miniBatchSize=10")
    @Test
    public void testInnerJoinWithNoUk() throws Exception {
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
                        // size = 5
                        insertRecord(
                                "Ord#3", "LineOrd#3", "5 Bellevue Drive, Pottstown, PD 19464"), // x
                        updateBeforeRecord(
                                "Ord#3", "LineOrd#3", "5 Bellevue Drive, Pottstown, PD 19464"), // x
                        // size = 5
                        updateBeforeRecord(
                                "Ord#9",
                                "LineOrd#9",
                                "11 Bellevue Drive, Pottstown, PI 19464"), // 5x
                        updateBeforeRecord(
                                "Ord#9",
                                "LineOrd#9",
                                "11 Bellevue Drive, Pottstown, PI 19464"), // 3x
                        updateAfterRecord(
                                "Ord#10", "LineOrd#10", "14 Bellevue Drive, Pottstown, PJ 19464"),
                        updateAfterRecord(
                                "Ord#18", "LineOrd#18", "22 Bellevue Drive, Pottstown, PK 19464"),
                        // size = 9
                        deleteRecord(
                                "Ord#1",
                                "LineOrd#2",
                                "4 Bellevue Drive, Pottstown, PB 19464"), // 2x
                        updateAfterRecord(
                                "Ord#9",
                                "LineOrd#9",
                                "11 Bellevue Drive, Pottstown, PI 19464"), // 3x
                        // size = 7
                        deleteRecord(
                                "Ord#6",
                                "LineOrd#6",
                                "8 Bellevue Drive, Pottstown, PF 19464"), // 4x
                        insertRecord(
                                "Ord#6",
                                "LineOrd#6",
                                "8 Bellevue Drive, Pottstown, PF 19464"), // 4x
                        // size = 7
                        updateAfterRecord(
                                "Ord#9",
                                "LineOrd#9",
                                "11 Bellevue Drive, Pottstown, PI 19464")); // 5x
        // size = 6 max = 9
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

    @Tag("miniBatchSize=3")
    @Test
    public void testLeftJoinWithJkContainsUk() throws Exception {}

    @Tag("miniBatchSize=3")
    @Test
    public void testLeftJoinWithHasUk() throws Exception {}

    @Tag("miniBatchSize=3")
    @Test
    public void testLeftJoinWithNoUk() throws Exception {}

    @Tag("miniBatchSize=3")
    @Test
    public void testRightJoinWithJkContainsUk() throws Exception {}

    @Tag("miniBatchSize=3")
    @Test
    public void testRightJoinWithHasUk() throws Exception {}

    @Tag("leftStateRetentionTime=4000")
    @Tag("miniBatchSize=3")
    @Test
    public void testRightJoinWithNoUk() throws Exception {}

    private static final Function<String, Boolean[]> JOIN_TYPE_EXTRACTOR =
            (testDisplayName) -> {
                if (testDisplayName.contains("InnerJoin")) {
                    return new Boolean[] {false, false};
                } else if (testDisplayName.contains("LeftOuterJoin")) {
                    return new Boolean[] {true, false};
                } else {
                    return new Boolean[] {false, true};
                }
            };

    private static final Function<String, FlinkJoinType> FLINK_JOIN_TYPE_EXTRACTOR =
            (testDisplayName) -> {
                if (testDisplayName.contains("InnerJoin")) {
                    return FlinkJoinType.INNER;
                } else if (testDisplayName.contains("LeftOuterJoin")) {
                    return FlinkJoinType.LEFT;
                } else {
                    return FlinkJoinType.RIGHT;
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
