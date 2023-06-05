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

/** for the case that records contains uniqueKey while joinKey does not contain uniqueKey. */
public class MiniBatchBufferHasUk implements MiniBatchBuffer {

    /**
     * here do not consider the case that uk is equivalent but the joinKey is not equivalent. all
     * things only obey the uk. So the store style need to be reconsidered.
     * Map(JoinKey,Map(UniqueKey,Input)).
     */
    //    private final transient Map<RowData, Map<RowData, List<RowData>>> bundle;

    /**
     * record the uKey and jKey mapping relation. the list size cannot be greater than 2; the order
     * is corresponding to the bundle. map(uk,list(jk)).
     */
    private final transient Map<RowData, List<RowData>> uKey2jKey;

    /**
     * respected result: records with a uniqueKey at most exist two. 1, no retractMsg: only one
     * accumulate Msg exists. 2, have retractMsg: one retractMsg first(and then accumulateMsg)
     * exists. things could be a Uk mapping 2 Jks. +I / +U / -D +I / -U +I and the +U is another
     * record changed to this Uk after retract its old uk. -D +U / -U +U.
     *
     * <p>case 2 must obey the original sequence of records. just a
     * Map(JoinKey,Map(UniqueKey,Input)) could make mistake. example -U +U --> +U -U cause that the
     * jk ordered by the map. use map(uk,list(records)) ensure the order of the records.
     */
    private final transient Map<RowData, List<RowData>> bundle;

    private transient int count;

    public MiniBatchBufferHasUk() {
        this.bundle = new HashMap<>();
        this.uKey2jKey = new HashMap<>();
        this.count = 0;
    }

    public boolean isEmpty() {
        return count == 0;
    }

    public void clear() {
        bundle.clear();
        uKey2jKey.clear();
        count = 0;
    }

    public int size() {
        return count;
    }

    /**
     * here do not consider the case that uk is equivalent but the joinKey is not equivalent. all
     * things only obey the uk. So the store style need to be changed.
     *
     * <p>Fold the records with reverse order. The rule: before the last | last record | result
     * check if the uKey2jKey contains the key yes -> check if the joinKey is equivalent equivalent
     * -> go current logic path nonequivalent ->
     *
     * <p>+I +U (modify the joinKey and not the JoinKey) only keep the last(+U) +I -U/-D (the
     * joinKey may be not the same with +I ) clear both +U +U (modify the joinKey and not the
     * JoinKey) only keep the last(+U) +U -U/-D (modify the joinKey and not the JoinKey) clear both.
     *
     * <p>The retractMsg is only allowed at start of new miniBatch however this is not allowed to
     * fold cause the retractMsg need to be processed to retract the previous accumulateMsg in last
     * miniBatch. -U +U , -D +I/+U is not allowed to fold. where +I refers to {@link
     * RowKind#INSERT}, +U refers to {@link RowKind#UPDATE_AFTER}, -U refers to {@link
     * RowKind#UPDATE_BEFORE}, -D refers to {@link RowKind#DELETE}.
     */
    private void FoldRecord(RowData uk) {
        int size = bundle.get(uk).size();
        if (size < 2) {
            return;
        }
        int pre = size - 2, last = size - 1;
        if (RowDataUtil.isAccumulateMsg(bundle.get(uk).get(pre))) {
            if (RowDataUtil.isRetractMsg(bundle.get(uk).get(last))) {
                bundle.get(uk).remove(last);
                count--;
                uKey2jKey.get(uk).remove(last);
            }
            bundle.get(uk).remove(pre);
            count--;
            uKey2jKey.get(uk).remove(pre);
        }
    }

    /**
     * return true means the record is valid. the record is valid when it is the first one whenever
     * its type. Returns false if the last and record are +I +I, +U +I, -U -U/-D, -D -U/-D which +I
     * refers to {@link RowKind#INSERT}, +U refers to {@link RowKind#UPDATE_AFTER}, -U refers to
     * {@link RowKind#UPDATE_BEFORE}, -D refers to {@link RowKind#DELETE}.
     */
    private boolean checkInvalid(RowData uk, RowData record) {
        RowData last = getLastOne(uk);
        if (last == null) {
            return true;
        } else {
            if (RowDataUtil.isAccumulateMsg(last)) {
                return !RowDataUtil.isInsertMsg(record);
            } else {
                return !RowDataUtil.isRetractMsg(record);
            }
        }
    }

    private boolean isContainNoUk(RowData uk) {
        return !bundle.containsKey(uk);
    }

    private void addUniquekey(RowData uk) {
        if (bundle.containsKey(uk)) {
            return;
        }
        List<RowData> val = new ArrayList<>();
        bundle.put(uk, val);
        List<RowData> jks = new ArrayList<>();
        uKey2jKey.put(uk, jks);
    }

    private RowData getLastOne(RowData uk) {
        int size = bundle.get(uk).size();
        if (size == 0) {
            return null;
        }
        return bundle.get(uk).get(size - 1);
    }

    @Override
    public int addRecord(RowData jk, RowData uk, RowData record) throws Exception {
        if (isContainNoUk(uk)) {
            addUniquekey(uk);
        }
        if (checkInvalid(uk, record)) {
            bundle.get(uk).add(record);
            uKey2jKey.get(uk).add(jk);
            count++;
            FoldRecord(uk);
        } else {
            throw new TableException("MiniBatch join invalid record in MiniBatchBufferHasUk");
        }
        return count;
    }

    /** <param>jk should be null. */
    @Override
    public List<RowData> getListRecord(RowData jk, RowData uk) {
        return bundle.get(uk);
    }

    @Override
    public Map<RowData, List<RowData>> getMapRecords() {
        Map<RowData, List<RowData>> result = new HashMap<>();
        for (Map.Entry<RowData, List<RowData>> entry : uKey2jKey.entrySet()) {
            RowData uKey = entry.getKey();
            List<RowData> values = entry.getValue();
            for (int idx = 0; idx < values.size(); idx++) {
                if (!result.containsKey(values.get(idx))) {
                    List<RowData> vallist = new ArrayList<>();
                    result.put(values.get(idx), vallist);
                }
                result.get(values.get(idx)).add(bundle.get(uKey).get(idx));
            }
        }
        return result;
    }
}
