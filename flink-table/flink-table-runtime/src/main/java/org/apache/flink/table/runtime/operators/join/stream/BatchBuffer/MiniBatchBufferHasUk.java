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
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** for the case that records contains uniqueKey while joinKey does not contain uniqueKey. */
public class MiniBatchBufferHasUk implements MiniBatchBuffer {

    /**
     * here do not consider the case that uk is equivalent but the joinKey is not equivalent. all
     * things only obey the uk. So the store style need to be changed
     * Map(JoinKey,Map(UniqueKey,Input)).
     */
    private final transient Map<RowData, Map<RowData, List<RowData>>> bundle;

    /**
     * ideal result: a uniqueKey only map to a joinKey. especially for the case: a uniqueKey
     * contains different joinKey.
     */
    private final transient Map<RowData, RowData> uKey2jKey;

    private transient int count;

    public MiniBatchBufferHasUk() {
        this.bundle = new HashMap<>();
        this.uKey2jKey = new HashMap<>();
        this.count = 0;
    }

    /**
     * the inputRecord contains a new jk while its old uk is already stored with another jk in
     * record. that is the uk and jk mapping relation changes.
     */
    public boolean ukAlreadyHasJk(RowData jk, RowData uk) {
        return uKey2jKey.containsKey(uk) && !uKey2jKey.get(uk).equals(jk);
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
     * for the records containing a uk corresponds to multiple jks. 1. find the old jk and get the
     * map 2. eliminate the record of the map 3. add the new record if it is accMsg.
     */
    private void specialAddRecord(RowData newJk, RowData uk, RowData record) {
        RowData oldJk = uKey2jKey.get(uk);
        List<RowData> recordList = bundle.get(oldJk).get(uk);
        int size = recordList.size();
        //        RowData pre = recordList.get(size-1);
        if (size > 0) {
            bundle.get(oldJk).get(uk).remove(size - 1);
            count--;
        }
        if (bundle.get(oldJk).get(uk).isEmpty()) {
            bundle.get(oldJk).remove(uk);
        }

        uKey2jKey.put(uk, newJk);

        // the logic here is not the same as the regularFoldRecord.
        if (RowDataUtil.isAccumulateMsg(record)) {
            addJoinKey(newJk, uk);
            addUniquekey(newJk, uk);
            bundle.get(newJk).get(uk).add(record);
            count++;
        }
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
     * miniBatch. -U +U only keep the last(+U) -D +I/+U only keep the last(+I/+U). where +I refers
     * to {@link RowKind#INSERT}, +U refers to {@link RowKind#UPDATE_AFTER}, -U refers to {@link
     * RowKind#UPDATE_BEFORE}, -D refers to {@link RowKind#DELETE}.
     */
    private void regularFoldRecord(RowData jk, RowData uk) {
        int size = bundle.get(jk).get(uk).size();
        if (size < 2) {
            return;
        }
        int pre = size - 2, last = size - 1;
        switch (bundle.get(jk).get(uk).get(pre).getRowKind()) {
            case INSERT:
            case UPDATE_AFTER:
                if (RowDataUtil.isRetractMsg(bundle.get(jk).get(uk).get(last))) {
                    bundle.get(jk).get(uk).remove(last);
                    count--;
                }
                bundle.get(jk).get(uk).remove(pre);
                count--;
                break;
            case UPDATE_BEFORE:
            case DELETE:
                bundle.get(jk).get(uk).remove(pre);
                count--;
                break;
        }
    }

    /**
     * Returns false if the last and record are +I +I, +U +I, -U +I/-U/-D, -D -U/-D which +I refers
     * to {@link RowKind#INSERT}, +U refers to {@link RowKind#UPDATE_AFTER}, -U refers to {@link
     * RowKind#UPDATE_BEFORE}, -D refers to {@link RowKind#DELETE}.
     */
    private boolean checkInvalid(RowData jk, RowData uk, RowData record) {
        RowData last = getLastOne(jk, uk);
        if (last == null) {
            return !RowDataUtil.isRetractMsg(record);
        } else {
            switch (last.getRowKind()) {
                case INSERT:
                case UPDATE_AFTER:
                    if (RowDataUtil.isInsertMsg(record)) {
                        return false;
                    }
                    break;
                case UPDATE_BEFORE:
                    if (!RowDataUtil.isUAMsg(record)) {
                        return false;
                    }
                    break;
                case DELETE:
                    if (RowDataUtil.isRetractMsg(record)) {
                        return false;
                    }
                    break;
            }
        }
        return true;
    }

    private boolean isContainNoJoinKey(RowData jk) {
        return !bundle.containsKey(jk);
    }

    private boolean isContainNoUk(RowData jk, RowData uk) {
        return !bundle.get(jk).containsKey(uk);
    }

    private void addJoinKey(RowData jk, RowData uk) {
        if (bundle.containsKey(jk)) {
            return;
        }
        Map<RowData, List<RowData>> recMap = new HashMap<>();
        List<RowData> val = new ArrayList<>();
        recMap.put(uk, val);
        bundle.put(jk, recMap);
    }

    private void addUniquekey(RowData jk, RowData uk) {
        if (bundle.get(jk).containsKey(uk)) {
            return;
        }
        List<RowData> val = new ArrayList<>();
        bundle.get(jk).put(uk, val);
        uKey2jKey.put(uk, jk);
    }

    private RowData getLastOne(RowData jk, RowData uk) {
        if (ukAlreadyHasJk(jk, uk)) {
            jk = uKey2jKey.get(uk);
        }
        int size = bundle.get(jk).get(uk).size();
        if (size == 0) {
            return null;
        }
        return bundle.get(jk).get(uk).get(size - 1);
    }

    @Override
    public int addRecord(RowData jk, RowData uk, RowData record) throws Exception {
        if (ukAlreadyHasJk(jk, uk)) {
            specialAddRecord(jk, uk, record);
        } else {
            if (isContainNoJoinKey(jk)) {
                addJoinKey(jk, uk);
                addUniquekey(jk, uk);
            } else if (isContainNoUk(jk, uk)) {
                addUniquekey(jk, uk);
            }
            if (checkInvalid(jk, uk, record)) {
                bundle.get(jk).get(uk).add(record);
                count++;
                regularFoldRecord(jk, uk);
                //                return count;
            } else {
                throw new TableException("MiniBatch join invalid record in MiniBatchBufferHasUk");
            }
        }
        return count;
    }

    @Override
    public List<RowData> getListRecord(RowData jk, RowData uk) {
        return bundle.get(jk).get(uk);
    }

    @Override
    public Map<RowData, List<RowData>> getMapRecords() {
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
}
