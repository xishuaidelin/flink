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

package org.apache.flink.table.runtime.operators.join.stream.batchbuffer;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.RowDataUtil;
import org.apache.flink.types.RowKind;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** MiniBatchBufferHasUk for input has uniqueKey which is not equivalent to joinKey. */
public class MiniBatchBufferHasUk implements MiniBatchBuffer {

    //  Map<joinKey, Map<uniqueKey,records>>
    private transient Map<RowData, Map<RowData, List<RowData>>> bundle;

    private transient int count;

    private transient int foldSize;

    public MiniBatchBufferHasUk() {
        this.bundle = new HashMap<>();
        this.count = 0;
        this.foldSize = 0;
    }

    public boolean isEmpty() {
        return count == 0;
    }

    public void clear() {
        bundle.clear();
        count = 0;
        foldSize = 0;
    }

    public int processedSize() {
        return count;
    }

    /**
     * Fold the records with reverse order. The rule before the last | last record | result +I/+U
     * +U/+I only keep the last(+U) +I/+U -U/-D clear both -U/-D +U/+I only keep the last(+U) where
     * +I refers to {@link RowKind#INSERT}, +U refers to {@link RowKind#UPDATE_AFTER}, -U refers to
     * {@link RowKind#UPDATE_BEFORE}, -D refers to {@link RowKind#DELETE}.
     */
    private void foldRecord(RowData jk, RowData uk) {
        List<RowData> list = bundle.get(jk).get(uk);
        int size = list.size();
        if (size < 2) {
            return;
        }
        int pre = size - 2, last = size - 1;
        if (RowDataUtil.isAccumulateMsg(list.get(pre))) {
            if (RowDataUtil.isRetractMsg(list.get(last))) {
                list.remove(last);
                foldSize++;
            }
            list.remove(pre);
            foldSize++;
            if (list.isEmpty()) {
                bundle.get(jk).remove(uk);
            }
        }
        // do not deal with cases that pre before the input is retractMsg
    }

    private boolean isContainNoJoinKey(RowData jk) {
        return !bundle.containsKey(jk);
    }

    private boolean isContainNoUk(RowData jk, RowData uk) {
        return !bundle.get(jk).containsKey(uk);
    }

    private void addJoinKey(RowData jk, RowData uk) {
        Map<RowData, List<RowData>> recMap = new HashMap<>();
        List<RowData> val = new ArrayList<>();
        recMap.put(uk, val);
        bundle.put(jk, recMap);
    }

    private void addUniqueKey(RowData jk, RowData uk) {
        List<RowData> val = new ArrayList<>();
        bundle.get(jk).put(uk, val);
    }

    @Override
    public int addRecord(RowData jk, RowData uk, RowData record) throws Exception {
        if (isContainNoJoinKey(jk)) {
            addJoinKey(jk, uk);
        } else if (isContainNoUk(jk, uk)) {
            addUniqueKey(jk, uk);
        }
        bundle.get(jk).get(uk).add(record);
        count++;
        foldRecord(jk, uk);
        return count;
    }

    @Override
    public List<RowData> getRecordsWithUk(RowData jk, RowData uk) {
        return bundle.get(jk).get(uk);
    }

    @Override
    public Map<RowData, List<RowData>> getRecordsWithJk() {
        Map<RowData, List<RowData>> result = new HashMap<>();
        for (Map.Entry<RowData, Map<RowData, List<RowData>>> entry : bundle.entrySet()) {
            RowData key = entry.getKey();
            List<RowData> val = new ArrayList<>();
            Collection<List<RowData>> values = entry.getValue().values();
            for (List<RowData> value : values) {
                val.addAll(value);
            }
            result.put(key, val);
        }
        return result;
    }

    @Override
    public int getFoldSize() {
        return foldSize;
    }
}
