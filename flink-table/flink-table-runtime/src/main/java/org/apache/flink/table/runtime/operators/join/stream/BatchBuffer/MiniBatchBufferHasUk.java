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
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MiniBatchBufferHasUk implements MiniBatchBuffer {

    /** Map<JoinKey, Map<UniqueKey,Input>> */
    private transient Map<RowData, Map<RowData, List<RowData>>> bundle;

    private transient int count;

    public MiniBatchBufferHasUk() {
        this.bundle = new HashMap<>();
        this.count = 0;
    }

    public boolean isEmpty() {
        return count == 0;
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
    private void foldRecord(RowData Jk, RowData Uk) {
        int size = bundle.get(Jk).get(Uk).size();
        if (size < 2) return;
        int pre = size - 2, last = size - 1;
        switch (bundle.get(Jk).get(Uk).get(pre).getRowKind()) {
            case INSERT:
            case UPDATE_AFTER:
                bundle.get(Jk).get(Uk).remove(pre);
                count--;
                if (RowDataUtil.isRetractMsg(bundle.get(Jk).get(Uk).get(last))) {
                    bundle.get(Jk).get(Uk).remove(last);
                    count--;
                }
                break;
            case UPDATE_BEFORE:
            case DELETE:
                bundle.get(Jk).get(Uk).remove(pre);
                count--;
                break;
        }
    }

    /**
     * Returns false if the last and record are
     *  +I +I,
     *  +U +I,
     *  -U +I/-U/-D,
     *  -D -U/-D
     *  which +I refers to {@link RowKind#INSERT,
     *        +U refers to {@link RowKind#UPDATE_AFTER},
     *        -U refers to {@link RowKind#UPDATE_BEFORE},
     *        -D refers to {@link RowKind#DELETE}.
     */
    private boolean checkInvalid(RowData last, RowData record) {
        switch (last.getRowKind()) {
            case INSERT:
                if (RowDataUtil.isInsertMsg(record)) return false;
                break;
            case UPDATE_AFTER:
                if (RowDataUtil.isInsertMsg(record)) return false;
                break;
            case UPDATE_BEFORE:
                if (!RowDataUtil.isUAMsg(record)) return false;
                break;
            case DELETE:
                if (RowDataUtil.isRetractMsg(record)) return false;
                break;
        }
        return true;
    }

    private boolean isContainNoJoinKey(RowData Jk) {
        return !bundle.containsKey(Jk);
    }

    private boolean isContainNoUk(RowData Jk, RowData Uk) {
        return !bundle.get(Jk).containsKey(Uk);
    }

    private void addJoinKey(RowData Jk, RowData Uk) {
        Map<RowData, List<RowData>> rec_map = new HashMap<>();
        List<RowData> val = new ArrayList<>();
        rec_map.put(Uk, val);
        bundle.put(Jk, rec_map);
    }

    private void addUk(RowData Jk, RowData Uk) {
        List<RowData> val = new ArrayList<>();
        bundle.get(Jk).put(Uk, val);
    }

    private RowData getLastOne(RowData Jk, RowData Uk) {
        return bundle.get(Jk).get(Uk).get(bundle.get(Jk).get(Uk).size() - 1);
    }

    @Override
    public int addRecord(RowData Jk, RowData Uk, RowData record) throws Exception {
        RowData last = getLastOne(Jk, Uk);
        if (checkInvalid(last, record)) {
            if (isContainNoJoinKey(Jk)) {
                addJoinKey(Jk, Uk);
            }
            if (isContainNoUk(Jk, Uk)) {
                addUk(Jk, Uk);
            }
            bundle.get(Jk).get(Uk).add(record);
            count++;
            foldRecord(Jk, Uk);
            return count;
        } else {
            throw new RuntimeException("MiniBatch join err in MiniBatchBufferHasUk");
        }
    }

    @Override
    public List<RowData> getListRecord(RowData Jk, RowData Uk) {
        return bundle.get(Jk).get(Uk);
    }

    @Override
    public Map<RowData, List<RowData>> getMapRecords() {
        Map<RowData, List<RowData>> result = new HashMap<>();
        for (Map.Entry<RowData, Map<RowData, List<RowData>>> entry : bundle.entrySet()) {
            RowData Key = entry.getKey();
            List<RowData> Val = new ArrayList<>();
            Collection<List<RowData>> values = entry.getValue().values();
            for (List<RowData> value : values) {
                Val.addAll(value);
            }
            result.put(Key, Val);
        }
        return result;
    }
}
