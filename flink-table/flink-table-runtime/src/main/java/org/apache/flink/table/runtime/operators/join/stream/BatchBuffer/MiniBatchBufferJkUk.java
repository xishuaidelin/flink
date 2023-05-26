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

package org.apache.flink.table.runtime.operators.join.stream.BatchBuffer;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.RowDataUtil;
import org.apache.flink.types.RowKind;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MiniBatchBufferJkUk implements MiniBatchBuffer {

    private transient Map<RowData, List<RowData>> bundle;

    private transient int count;

    public MiniBatchBufferJkUk() {
        this.bundle = new HashMap<>();
        this.count = 0;
    }

    public boolean isEmpty() {
        return true;
    }

    public void clear() {
        bundle.clear();
        count = 0;
    }

    public int size() {
        return count;
    }

    /**
     * Fold the records with reverse order. The rule:
     * before the last   |   last record   |   result
     *      +I                  +U              only keep the last(+U)
     *      +I                  -U/-D           clear both
     *      +U                  +U              only keep the last(+U)
     *      +U                  -U/-D           clear both
     *      -U                  +U              only keep the last(+U)
     *      -D                  +I/+U           only keep the last(+I/+U)
     * where +I refers to {@link RowKind#INSERT,
     *       +U refers to {@link RowKind#UPDATE_AFTER},
     *       -U refers to {@link RowKind#UPDATE_BEFORE},
     *       -D refers to {@link RowKind#DELETE}.
     */
    private void foldRecord(RowData Jk) {
        int size = bundle.get(Jk).size();
        if (size < 2) return;
        int pre = size - 2, last = size - 1;
        switch (bundle.get(Jk).get(pre).getRowKind()) {
            case INSERT:
            case UPDATE_AFTER:
                bundle.get(Jk).remove(pre);
                count--;
                if (RowDataUtil.isRetractMsg(bundle.get(Jk).get(last))) {
                    bundle.get(Jk).remove(last);
                    count--;
                }
                break;
            case UPDATE_BEFORE:
            case DELETE:
                bundle.get(Jk).remove(pre);
                count--;
                break;
        }
    }

    /**
     * Returns false if the last and record are
     *  +I +I,
     *  +U +I,
     *  -U -U/-D  // -U +I is reasonable.
     *            // cause the -U means modifying the Uk and +I means the record inserting into with the same Uk
     *  -D -U/-D
     *  which +I refers to {@link RowKind#INSERT,
     *        +U refers to {@link RowKind#UPDATE_AFTER},
     *        -U refers to {@link RowKind#UPDATE_BEFORE},
     *        -D refers to {@link RowKind#DELETE}.
     */
    private boolean checkInvalid(RowData last, RowData record) {
        switch (last.getRowKind()) {
            case INSERT:
            case UPDATE_AFTER:
                if (RowDataUtil.isInsertMsg(record)) return false;
                break;
            case UPDATE_BEFORE:
            case DELETE:
                if (RowDataUtil.isRetractMsg(record)) return false;
                break;
        }
        return true;
    }

    private boolean isContainNoJoinKey(RowData Jk) {
        return !bundle.containsKey(Jk);
    }

    private void addJoinKey(RowData Jk) {
        List<RowData> val = new ArrayList<>();
        bundle.put(Jk, val);
    }

    private RowData getLastOne(RowData Jk) {
        return bundle.get(Jk).get(bundle.get(Jk).size() - 1);
    }

    @Override
    public int addRecord(RowData Jk, RowData Uk, RowData record) throws Exception {
        RowData last = getLastOne(Jk);
        if (checkInvalid(last, record)) {
            if (isContainNoJoinKey(Jk)) {
                addJoinKey(Jk);
            }
            bundle.get(Jk).add(record);
            count++;
            foldRecord(Jk);
            return count;
        } else {
            throw new RuntimeException("MiniBatch join err in MiniBatchBufferJkUk");
        }
    }

    @Override
    public List<RowData> getListRecord(RowData Jk, RowData Uk) {
        return bundle.get(Jk);
    }

    @Override
    public Map<RowData, List<RowData>> getMapRecords() {
        return bundle;
    }
}
