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
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

/** for the case that records have no uniqueKey. */
public class MiniBatchBufferNoUk implements MiniBatchBuffer {
    /**
     * Are there at most two input records that the key is equivalent ? Table could have multiple
     * rows which is completely equivalent. +I 1 A B +I 1 A B +I 1 A B As shown above, same
     * statements may occur many times. This could be abstracted to two modes: accumulate and
     * retract. The bundle only stores the accumulate records. When the retract record occurs it
     * would find the corresponding records(accumulate) and remove it.
     */
    private transient Map<RowData, List<RowData>> bundle;

    /** if status[jk].get[pos] = false, then bundle[jk].get[pos] is folded. */
    private transient Map<RowData, List<Boolean>> status;

    private transient int count;
    private transient int foldSize;
    /**
     * map(Jk, map(FieldsHash,List(pos))) FieldsHash : the hash of fields of input except the
     * RowKind. List[pos]pos : input corresponding position in the bundle valueList. cause there
     * maybe repetitive rows.
     */
    private transient Map<RowData, Map<Integer, List<Integer>>> dic;

    public MiniBatchBufferNoUk() {
        this.bundle = new HashMap<>();
        this.status = new HashMap<>();
        this.dic = new HashMap<>();
        this.count = 0;
        this.foldSize = 0;
    }

    public boolean isEmpty() {
        return count == 0;
    }

    public void clear() {
        count = 0;
        foldSize = 0;
        bundle.clear();
        status.clear();
        dic.clear();
    }

    public int size() {
        return count;
    }

    public int getFoldSize() {
        return foldSize;
    }

    private boolean isContainNoJoinKey(RowData jk) {
        return !bundle.containsKey(jk);
    }

    private boolean isContainNoHashKey(RowData jk, Integer hk) {
        return !dic.get(jk).containsKey(hk);
    }

    private void addJoinKey(RowData jk) {
        List<RowData> val = new ArrayList<>();
        bundle.put(jk, val);
        List<Boolean> sta = new ArrayList<>();
        status.put(jk, sta);
        Map<Integer, List<Integer>> mp = new HashMap<>();
        dic.put(jk, mp);
    }

    private void addHashKey(RowData jk, Integer hk) {
        List<Integer> dicVal = new ArrayList<>();
        dic.get(jk).put(hk, dicVal);
    }

    private void addItem(RowData jk, RowData record, int hashKey) {
        bundle.get(jk).add(record);
        status.get(jk).add(true);
        dic.get(jk).get(hashKey).add(bundle.get(jk).size() - 1);
    }

    /**
     * Fold the records only in accumulate and retract modes. The rule: the input is accumulateMsg
     * -> check if there is retractMsg before in the same hashKey if yes tnen fold that else add
     * input to the bundle. the input is retractMsg -> remove the accumulateMsg in the same HashKey
     * from bundle. The same HashKey means that the input's field values are completely equivalent.
     *
     * <p>accumulateMsg refers to +I/+U which refers to {@link RowKind#INSERT}/{@link
     * RowKind#UPDATE_AFTER}, retractMsg refers to -U/-D which refers to {@link
     * RowKind#UPDATE_BEFORE}/{@link RowKind#DELETE}.
     */
    private void foldRecord(RowData jk, int hashKey, RowData record) {
        List<Integer> idx = dic.get(jk).get(hashKey);
        // new added record is the current record
        ListIterator<Integer> iterator = idx.listIterator(idx.size() - 1);
        while (iterator.hasPrevious()) {
            int pos = iterator.previous();
            if (status.get(jk).get(pos)) {
                RowData preRecord = bundle.get(jk).get(pos);
                if ((RowDataUtil.isAccumulateMsg(record) && RowDataUtil.isRetractMsg(preRecord))
                        || (RowDataUtil.isRetractMsg(record)
                                && RowDataUtil.isAccumulateMsg(preRecord))) {
                    // pre_record & current record is folded
                    status.get(jk).set(pos, false);
                    status.get(jk).set(status.get(jk).size() - 1, false);
                    foldSize += 2;
                    break;
                }
            }
        }
    }

    /**
     * Returns false if there is no accumulateMsg before retractMsg in the same HashKey. this should
     * be tolerated cause the accumulateMsg could be in last miniBatch.
     */
    private boolean checkInvalid() {
        return true;
    }

    /** here the uk is null. */
    @Override
    public int addRecord(RowData jk, RowData uk, RowData record) throws Exception {
        if (isContainNoJoinKey(jk)) {
            addJoinKey(jk);
        }
        RowKind type = record.getRowKind();
        record.setRowKind(RowKind.INSERT);
        int hashKey = record.hashCode();
        if (isContainNoHashKey(jk, hashKey)) {
            addHashKey(jk, hashKey);
        }
        if (checkInvalid()) {
            record.setRowKind(type);
            addItem(jk, record, hashKey);
            count++;
            foldRecord(jk, hashKey, record);
        } else {
            throw new TableException("MiniBatch join invalid record in MiniBatchBufferNoUk");
        }
        return count;
    }

    @Override
    public List<RowData> getRecordsWithUk(RowData jk, RowData uk) {
        List<RowData> result = new ArrayList<>();
        Iterator<RowData> recIter = bundle.get(jk).iterator();
        for (Boolean aBoolean : status.get(jk)) {
            RowData record = recIter.next();
            if (aBoolean) {
                result.add(record);
            }
        }
        return result;
    }

    @Override
    public Map<RowData, List<RowData>> getRecordsWithJk() {
        for (Map.Entry<RowData, List<RowData>> entry : bundle.entrySet()) {
            RowData jk = entry.getKey();
            assert status.get(jk).size() == entry.getValue().size();
            Iterator<RowData> recIter = entry.getValue().iterator();
            Iterator<Boolean> statusIter = status.get(jk).iterator();
            while (statusIter.hasNext()) {
                boolean exist = statusIter.next();
                if (exist) {
                    recIter.next();
                } else {
                    statusIter.remove();
                    recIter.next();
                    recIter.remove();
                }
            }
        }
        return bundle;
    }
}
