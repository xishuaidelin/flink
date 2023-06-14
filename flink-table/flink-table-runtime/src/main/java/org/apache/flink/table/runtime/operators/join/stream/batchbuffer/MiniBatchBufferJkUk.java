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

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.RowDataUtil;
import org.apache.flink.types.RowKind;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** for the case that joinKey contains uniqueKey. */
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

    public int size() {
        return count;
    }

    public int getFoldSize() {
        return foldSize;
    }

    /**
     * Fold the records with reverse order. The rule: before the last ｜ last record ｜ result +I ｜ +U
     * ｜ only keep the last(+U) +I ｜ -U/-D ｜ clear both +U ｜ +U ｜ only keep the last(+U) +U ｜ -U/-D
     * ｜ clear both where +I refers to {@link RowKind#INSERT}, +U refers to {@link
     * RowKind#UPDATE_AFTER}, -U refers to {@link RowKind#UPDATE_BEFORE}, -D refers to {@link
     * RowKind#DELETE}.
     */
    private void foldRecord(RowData jk) {
        int size = bundle.get(jk).size();
        if (size < 2) {
            return;
        }
        int pre = size - 2, last = size - 1;
        if (RowDataUtil.isAccumulateMsg(bundle.get(jk).get(pre))) {
            if (RowDataUtil.isRetractMsg(bundle.get(jk).get(last))) {
                bundle.get(jk).remove(last);
                //                count--;
                foldSize++;
            }
            bundle.get(jk).remove(pre);
            //            count--;
            foldSize++;
            if (bundle.get(jk).isEmpty()) {
                bundle.remove(jk);
            }
        }
    }

    /**
     * Returns false(Invalid) if the last one and current record are +I +I, +U +I, -U/-D -U/-D. -U
     * +I is reasonable cause the -U means modifying the Uk and +I means new record inserting into
     * with the same Uk.
     *
     * <p>+I refers to {@link RowKind#INSERT}, +U refers to {@link RowKind#UPDATE_AFTER}, -U refers
     * to {@link RowKind#UPDATE_BEFORE}, -D refers to {@link RowKind#DELETE}.
     */
    private boolean checkInvalid(RowData last, RowData record) {
        // first one of the batch could be a record with any type
        if (last == null) {
            return true;
        } else {
            if (RowDataUtil.isAccumulateMsg(last)) {
                // +I/+U +I is invalid
                return !RowDataUtil.isInsertMsg(record);
            } else {
                // -U/-D -U/-D is invalid
                return !RowDataUtil.isRetractMsg(record);
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

    private RowData getLastOne(RowData jk) {
        int size = bundle.get(jk).size();
        if (size == 0) {
            return null;
        }
        return bundle.get(jk).get(size - 1);
    }

    @Override
    public int addRecord(RowData jk, RowData uk, RowData record) throws Exception {
        if (isContainNoJoinKey(jk)) {
            addJoinKey(jk);
        }
        RowData last = getLastOne(jk);
        if (checkInvalid(last, record)) {
            bundle.get(jk).add(record);
            count++;
            foldRecord(jk);
        } else {
            throw new TableException("MiniBatch join invalid record in MiniBatchBufferJkUk.");
        }
        return count;
    }

    @Override
    public List<RowData> getListRecord(RowData jk, RowData uk) {
        List<RowData> res = bundle.get(jk);
        assert res.size() == 1;
        return res;
    }

    @Override
    public Map<RowData, List<RowData>> getMapRecords() {
        return bundle;
    }
}
