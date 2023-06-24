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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * For the case that joinKey contains uniqueKey. The size of records in state is not more than 1.
 */
public class MiniBatchBufferJkUk implements MiniBatchBuffer {

    private transient Map<RowData, List<RowData>> bundle;

    private transient int count;
    private transient int foldSize;

    public MiniBatchBufferJkUk() {
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

    public int getFoldSize() {
        return foldSize;
    }

    /**
     * Fold the records with reverse order. The rule: before the last | last record | result +I/+U
     * +U/+I only keep the last(+U/+I) +I/+U -U/-D clear both -U/-D +U/+I only keep the last(+U/+I)
     * where +I refers to {@link RowKind#INSERT}, +U refers to {@link RowKind#UPDATE_AFTER}, -U
     * refers to {@link RowKind#UPDATE_BEFORE}, -D refers to {@link RowKind#DELETE}.
     */
    private void foldRecord(RowData jk) {
        List<RowData> list = bundle.get(jk);
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
                bundle.remove(jk);
            }
        }
    }

    private boolean isContainNoJoinKey(RowData jk) {
        return !bundle.containsKey(jk);
    }

    private void addJoinKey(RowData jk) {
        List<RowData> val = new ArrayList<>();
        bundle.put(jk, val);
    }

    @Override
    public int addRecord(RowData jk, RowData uk, RowData record) throws Exception {
        if (isContainNoJoinKey(jk)) {
            addJoinKey(jk);
        }
        bundle.get(jk).add(record);
        count++;
        foldRecord(jk);
        return count;
    }

    @Override
    public List<RowData> getRecordsWithUk(RowData jk, RowData uk) {
        List<RowData> res = bundle.get(jk);
        assert res.size() == 1;
        return res;
    }

    @Override
    public Map<RowData, List<RowData>> getRecordsWithJk() {
        return bundle;
    }
}
