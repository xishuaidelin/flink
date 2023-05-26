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
import org.apache.flink.table.runtime.operators.join.stream.batchbuffer.MiniBatchBufferHasUk;
import org.apache.flink.table.runtime.operators.join.stream.batchbuffer.MiniBatchBufferJkUk;
import org.apache.flink.table.runtime.operators.join.stream.batchbuffer.MiniBatchBufferNoUk;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;

import static org.apache.flink.table.runtime.util.StreamRecordUtils.deleteRecord;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.insertRecord;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.updateAfterRecord;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.updateBeforeRecord;

/** Test for MiniBatch buffer only which verify the logic of folding in MiniBatch. */
public class BatchBufferTest extends BatchBufferTestBase {

    /** for JkContainsUk the left records only contain +I +U and only one record for a JoinKey. */
    private void compareJkContainsUkRecords(List<StreamRecord<RowData>> leftAfterFold)
            throws Exception {
        assert joinKeySelector != null;
        //        assert buffer.size() == leftAfterFold.size();
        for (StreamRecord<RowData> row : leftAfterFold) {
            RowData joinKey = joinKeySelector.getKey(row.getValue());
            List<RowData> res = buffer.getRecordsWithUk(joinKey, null);
            assert !res.isEmpty();
            assert row.getValue().equals(res.get(0));
        }
    }

    private void compareHasUkRecords(List<StreamRecord<RowData>> leftAfterFold) throws Exception {
        // check the list of record which is defined by joinKey and hashKey.
        assert joinKeySelector != null;
        //        assert buffer.size() == leftAfterFold.size();
        for (StreamRecord<RowData> row : leftAfterFold) {
            RowData joinKey = joinKeySelector.getKey(row.getValue());
            RowData uniqueKey = inputKeySelector2.getKey(row.getValue());
            List<RowData> res = buffer.getRecordsWithUk(joinKey, uniqueKey);
            assert !res.isEmpty();
            assert res.contains(row.getValue());
        }
    }

    /** */
    private void compareHasNoUkRecords(List<StreamRecord<RowData>> leftAfterFold) throws Exception {
        assert joinKeySelector != null;
        //        assert buffer.size() == leftAfterFold.size();
        for (StreamRecord<RowData> row : leftAfterFold) {
            RowData joinKey = joinKeySelector.getKey(row.getValue());
            List<RowData> res = buffer.getRecordsWithUk(joinKey, null);
            assert !res.isEmpty();
            assert res.contains(row.getValue());
        }
    }

    private Callable<?> addSingleRecord(StreamRecord<RowData> record) throws Exception {
        RowData input = record.getValue();
        assert joinKeySelector != null;
        RowData joinKey = joinKeySelector.getKey(input);
        RowData uniqueKey = null;
        if (buffer instanceof MiniBatchBufferHasUk) {
            assert inputSpecHasUk.getUniqueKeySelector() != null;
            uniqueKey = inputSpecHasUk.getUniqueKeySelector().getKey(input);
        }
        buffer.addRecord(joinKey, uniqueKey, input);
        return null;
    }

    private void addRecordList(List<StreamRecord<RowData>> input) throws Exception {
        for (StreamRecord<RowData> rec : input) {
            addSingleRecord(rec);
        }
    }

    /** JoinKey is order_id. */
    @Test
    public void testJkContainsUkValid() throws Exception {
        buffer = new MiniBatchBufferJkUk();
        //  Already considered these cases (in the same joinKey).
        // +-------------------+------------------------------------------------+
        // |        Case       |                     Validity                   |
        // +-------------------+------------------------------------------------+
        // |        +I -U/-D/+U |                      Valid                    |
        // +-------------------+------------------------------------------------+
        // |        +U -U/-D/+U |                      Valid                    |
        // +-------------------+------------------------------------------------+
        // |        -D +I/+U    |                      Valid                    |
        // +-------------------+------------------------------------------------+
        // |        -U +I/+U    |                      Valid                    |
        // +-------------------+------------------------------------------------+
        List<StreamRecord<RowData>> records =
                Arrays.asList(
                        insertRecord("Ord#1", "LineOrd#1", "3 Bellevue Drive, Pottstown, PA 19464"),
                        insertRecord("Ord#2", "LineOrd#2", "4 Bellevue Drive, Pottstown, PB 19464"),
                        insertRecord("Ord#3", "LineOrd#3", "5 Bellevue Drive, Pottstown, PC 19464"),
                        insertRecord("Ord#4", "LineOrd#4", "6 Bellevue Drive, Pottstown, PD 19464"),
                        insertRecord("Ord#5", "LineOrd#5", "7 Bellevue Drive, Pottstown, PE 19464"),
                        insertRecord("Ord#6", "LineOrd#6", "8 Bellevue Drive, Pottstown, PF 19464"),
                        insertRecord("Ord#7", "LineOrd#7", "9 Bellevue Drive, Pottstown, PG 19464"),
                        insertRecord(
                                "Ord#8", "LineOrd#8", "10 Bellevue Drive, Pottstown, PH 19464"),
                        insertRecord(
                                "Ord#9", "LineOrd#9", "11 Bellevue Drive, Pottstown, PI 19464"),
                        updateBeforeRecord(
                                "Ord#5", "LineOrd#5", "7 Bellevue Drive, Pottstown, PE 19464"),
                        updateBeforeRecord(
                                "Ord#9", "LineOrd#9", "11 Bellevue Drive, Pottstown, PI 19464"),
                        updateBeforeRecord(
                                "Ord#7", "LineOrd#7", "9 Bellevue Drive, Pottstown, PG 19464"),
                        updateAfterRecord(
                                "Ord#10", "LineOrd#10", "14 Bellevue Drive, Pottstown, PJ 19464"),
                        updateAfterRecord(
                                "Ord#18", "LineOrd#18", "22 Bellevue Drive, Pottstown, PK 19464"),
                        updateAfterRecord(
                                "Ord#14", "LineOrd#14", "18 Bellevue Drive, Pottstown, PL 19464"),
                        deleteRecord("Ord#4", "LineOrd#4", "6 Bellevue Drive, Pottstown, PD 19464"),
                        deleteRecord("Ord#6", "LineOrd#6", "8 Bellevue Drive, Pottstown, PF 19464"),
                        updateAfterRecord(
                                "Ord#1", "LineOrd#x1", "x3 Bellevue Drive, Pottstown, PAxx 19464"),
                        updateAfterRecord(
                                "Ord#2", "LineOrd#x2", "x4 Bellevue Drive, Pottstown, PBxx 19464"),
                        updateAfterRecord(
                                "Ord#3", "LineOrd#x3", "x5 Bellevue Drive, Pottstown, PCxx 19464"),
                        updateAfterRecord(
                                "Ord#10",
                                "LineOrd#100y",
                                "14y0 Bellevue Drive, Pottstown, PJyy 19464"),
                        updateBeforeRecord(
                                "Ord#10",
                                "LineOrd#100y",
                                "14y0 Bellevue Drive, Pottstown, PJyy 19464"),
                        deleteRecord(
                                "Ord#3", "LineOrd#x3", "x5 Bellevue Drive, Pottstown, PCxx 19464"),
                        insertRecord(
                                "Ord#3", "LineOrd#x3", "x5 Bellevue Drive, Pottstown, PCxx 19464"),
                        insertRecord(
                                "Ord#10",
                                "LineOrd#100y",
                                "14y0 Bellevue Drive, Pottstown, PJyy 19464"),
                        updateAfterRecord(
                                "Ord#7", "LineOrd#7", "9 Bellevue Drive, Pottstown, PG 19464"),
                        updateAfterRecord(
                                "Ord#4", "LineOrd#4", "6 Bellevue Drive, Pottstown, PD 19464"));
        addRecordList(records);
        List<StreamRecord<RowData>> result =
                Arrays.asList(
                        insertRecord(
                                "Ord#8", "LineOrd#8", "10 Bellevue Drive, Pottstown, PH 19464"),
                        updateAfterRecord(
                                "Ord#18", "LineOrd#18", "22 Bellevue Drive, Pottstown, PK 19464"),
                        updateAfterRecord(
                                "Ord#14", "LineOrd#14", "18 Bellevue Drive, Pottstown, PL 19464"),
                        updateAfterRecord(
                                "Ord#1", "LineOrd#x1", "x3 Bellevue Drive, Pottstown, PAxx 19464"),
                        updateAfterRecord(
                                "Ord#2", "LineOrd#x2", "x4 Bellevue Drive, Pottstown, PBxx 19464"),
                        insertRecord(
                                "Ord#3", "LineOrd#x3", "x5 Bellevue Drive, Pottstown, PCxx 19464"),
                        insertRecord(
                                "Ord#10",
                                "LineOrd#100y",
                                "14y0 Bellevue Drive, Pottstown, PJyy 19464"),
                        updateAfterRecord(
                                "Ord#7", "LineOrd#7", "9 Bellevue Drive, Pottstown, PG 19464"),
                        updateAfterRecord(
                                "Ord#4", "LineOrd#4", "6 Bellevue Drive, Pottstown, PD 19464"));
        compareJkContainsUkRecords(result);
        assert buffer.getFoldSize() == records.size() - result.size();
    }

    /**
     * These cases are considered in follow: +I +U (modify the joinKey and not the JoinKey) only
     * keep the last(+U) +I -U/-D (the joinKey may be not the same with +I ) clear both
     *
     * <p>The retractMsg is only allowed at start of new miniBatch -U +U only keep the last(+U) -D
     * +I/+U only keep the last(+I/+U).
     */
    @Test
    public void testHasUniquekeyAccBeginValid() throws Exception {
        buffer = new MiniBatchBufferHasUk();
        List<StreamRecord<RowData>> records =
                Arrays.asList(
                        insertRecord("Ord#1", "LineOrd#1", "3 Bellevue Drive, Pottstown, PA 19464"),
                        updateAfterRecord(
                                "Ord#11", "LineOrd#10", "xxx Bellevue Drive, Pottstown, PJ 19464"),
                        insertRecord("Ord#6", "LineOrd#5", "7 Bellevue Drive, Pottstown, PE 19464"),
                        insertRecord("Ord#6", "LineOrd#6", "8 Bellevue Drive, Pottstown, PF 19464"),
                        updateBeforeRecord( // +I -U with the same joinKey
                                "Ord#1", "LineOrd#1", "3 Bellevue Drive, Pottstown, PA 19464"),
                        deleteRecord( // +I -D with the same joinKey
                                "Ord#6", "LineOrd#5", "7 Bellevue Drive, Pottstown, PE 19464"),
                        updateAfterRecord( // +I +U with the same joinKey
                                "Ord#6", "LineOrd#6", "9 Bellevue Drive, Pottstown, PE 19464"),
                        updateAfterRecord(
                                "Ord#xxxx",
                                "LineOrd#10",
                                "yyy Bellevue Drive, Pottstown, PJ 19464"));
        addRecordList(records);
        List<StreamRecord<RowData>> result =
                Arrays.asList(
                        updateAfterRecord(
                                "Ord#11", "LineOrd#10", "xxx Bellevue Drive, Pottstown, PJ 19464"),
                        updateAfterRecord( // +I +U with the same joinKey
                                "Ord#6", "LineOrd#6", "9 Bellevue Drive, Pottstown, PE 19464"),
                        updateAfterRecord(
                                "Ord#xxxx",
                                "LineOrd#10",
                                "yyy Bellevue Drive, Pottstown, PJ 19464"));
        compareHasUkRecords(result);
        assert buffer.getFoldSize() == records.size() - result.size();
    }

    @Test
    public void testHasUniquekeyRetractBeginValid() throws Exception {
        buffer = new MiniBatchBufferHasUk();
        List<StreamRecord<RowData>> records =
                Arrays.asList(
                        // retract a Msg that is already retracted with the same uk before is
                        // invalid.
                        deleteRecord("Ord#1", "LineOrd#2", "4 Bellevue Drive, Pottstown, PB 19464"),
                        updateBeforeRecord(
                                "Ord#4", "LineOrd#3", "5 Bellevue Drive, Pottstown, PC 19464"),
                        updateAfterRecord(
                                "Ord#2", "LineOrd#2", "14 Bellevue Drive, Pottstown, PJ 19464"),
                        updateBeforeRecord(
                                "Ord#1", "LineOrd#1", "3 Bellevue Drive, Pottstown, PA 19464"),
                        updateAfterRecord(
                                "Ord#6", "LineOrd#6", "8 Bellevue Drive, Pottstown, PF 19464"));
        addRecordList(records);
        List<StreamRecord<RowData>> result =
                Arrays.asList(
                        deleteRecord("Ord#1", "LineOrd#2", "4 Bellevue Drive, Pottstown, PB 19464"),
                        updateBeforeRecord(
                                "Ord#4", "LineOrd#3", "5 Bellevue Drive, Pottstown, PC 19464"),
                        updateAfterRecord(
                                "Ord#2", "LineOrd#2", "14 Bellevue Drive, Pottstown, PJ 19464"),
                        updateBeforeRecord(
                                "Ord#1", "LineOrd#1", "3 Bellevue Drive, Pottstown, PA 19464"),
                        updateAfterRecord(
                                "Ord#6", "LineOrd#6", "8 Bellevue Drive, Pottstown, PF 19464"));
        compareHasUkRecords(result);
        assert buffer.getFoldSize() == records.size() - result.size();
    }

    @Test
    public void testNoUniqueKey() throws Exception {
        buffer = new MiniBatchBufferNoUk();
        List<StreamRecord<RowData>> records =
                Arrays.asList(
                        // jk is equivalent
                        //    others are equivalent and nonequivalent.
                        insertRecord("Ord#1", "LineOrd#1", "3 Bellevue Drive, Pottstown, PA 19464"),
                        insertRecord("Ord#1", "LineOrd#2", "4 Bellevue Drive, Pottstown, PB 19464"),
                        insertRecord(
                                "Ord#1",
                                "LineOrd#2",
                                "4 Bellevue Drive, Pottstown, PB 19464"), // xx
                        deleteRecord( // this -D shouldn't be folded
                                "Ord#6", "LineOrd#6", "8 Bellevue Drive, Pottstown, PF 19464"),
                        insertRecord("Ord#3", "LineOrd#1", "3 Bellevue Drive, Pottstown, PA 19464"),
                        insertRecord(
                                "Ord#3", "LineOrd#3", "5 Bellevue Drive, Pottstown, PD 19464"), // x
                        updateBeforeRecord(
                                "Ord#3", "LineOrd#3", "5 Bellevue Drive, Pottstown, PD 19464"), // x
                        updateBeforeRecord( // this -U shouldn't be folded
                                "Ord#9",
                                "LineOrd#9",
                                "11 Bellevue Drive, Pottstown, PI 19464"), // y
                        updateBeforeRecord(
                                "Ord#9",
                                "LineOrd#9",
                                "11 Bellevue Drive, Pottstown, PI 19464"), // y
                        updateAfterRecord(
                                "Ord#10", "LineOrd#10", "14 Bellevue Drive, Pottstown, PJ 19464"),
                        updateAfterRecord(
                                "Ord#18", "LineOrd#18", "22 Bellevue Drive, Pottstown, PK 19464"),
                        deleteRecord(
                                "Ord#1",
                                "LineOrd#2",
                                "4 Bellevue Drive, Pottstown, PB 19464"), // xx
                        updateAfterRecord( // this -U shouldn't be folded
                                "Ord#9",
                                "LineOrd#9",
                                "11 Bellevue Drive, Pottstown, PI 19464"), // y
                        deleteRecord(
                                "Ord#6",
                                "LineOrd#6",
                                "8 Bellevue Drive, Pottstown, PF 19464"), // yy
                        insertRecord(
                                "Ord#6",
                                "LineOrd#6",
                                "8 Bellevue Drive, Pottstown, PF 19464"), // yy
                        updateBeforeRecord( // this -U shouldn't be folded
                                "Ord#9", "LineOrd#9", "11 Bellevue Drive, Pottstown, PI 19464"));
        addRecordList(records);

        List<StreamRecord<RowData>> result =
                Arrays.asList(
                        insertRecord("Ord#1", "LineOrd#1", "3 Bellevue Drive, Pottstown, PA 19464"),
                        insertRecord("Ord#1", "LineOrd#2", "4 Bellevue Drive, Pottstown, PB 19464"),
                        deleteRecord( // this -D shouldn't be folded
                                "Ord#6", "LineOrd#6", "8 Bellevue Drive, Pottstown, PF 19464"),
                        insertRecord("Ord#3", "LineOrd#1", "3 Bellevue Drive, Pottstown, PA 19464"),
                        updateBeforeRecord( // this -U shouldn't be folded
                                "Ord#9",
                                "LineOrd#9",
                                "11 Bellevue Drive, Pottstown, PI 19464"), // y
                        updateAfterRecord(
                                "Ord#10", "LineOrd#10", "14 Bellevue Drive, Pottstown, PJ 19464"),
                        updateAfterRecord(
                                "Ord#18", "LineOrd#18", "22 Bellevue Drive, Pottstown, PK 19464"),
                        updateBeforeRecord( // this -U shouldn't be folded
                                "Ord#9", "LineOrd#9", "11 Bellevue Drive, Pottstown, PI 19464") // y
                        );
        compareHasNoUkRecords(result);
        assert buffer.getFoldSize() == records.size() - result.size();
    }
}
