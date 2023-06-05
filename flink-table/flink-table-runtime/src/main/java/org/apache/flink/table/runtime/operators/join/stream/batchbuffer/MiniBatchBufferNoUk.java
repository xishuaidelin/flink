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

    private transient int count;

    /**
     * map(Jk, map(FieldsHash,List(po))) FieldsHash : the hash of fields of input except the
     * RowKind. List[pos]pos : input corresponding position in the bundle valueList. cause there
     * maybe repetitive rows.
     */
    private transient Map<RowData, Map<Integer, List<Integer>>> dic;

    public MiniBatchBufferNoUk() {
        this.bundle = new HashMap<>();
        this.dic = new HashMap<>();
        this.count = 0;
    }

    public boolean isEmpty() {
        return count == 0;
    }

    public void clear() {
        count = 0;
        bundle.clear();
        dic.clear();
    }

    public int size() {
        return count;
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
        Map<Integer, List<Integer>> mp = new HashMap<>();
        dic.put(jk, mp);
    }

    private void addHashKey(RowData jk, Integer hk) {
        List<Integer> dicVal = new ArrayList<>();
        dic.get(jk).put(hk, dicVal);
    }

    /**
     * Fold the records only in accumulate and retract modes. The rule: the input is accumulateMsg
     * -> check if there is retractMsg before yes fold that or no add to the bundle. the input is
     * retractMsg -> remove the accumulateMsg in the same HashKey from bundle
     *
     * <p>The same HashKey means that the input's field values are completely equivalent.
     *
     * <p>where accumulateMsg refers to +I/+U which refers to {@link RowKind#INSERT}/{@link
     * RowKind#UPDATE_AFTER}, accumulateMsg refers to -U/-D which refers to {@link
     * RowKind#UPDATE_BEFORE}/{@link RowKind#DELETE}.
     */
    private void foldRecord(RowData jk, RowKind type, int hashKey, RowData record) {
        int size = dic.get(jk).get(hashKey).size(), addPos;
        if (size > 0) {
            int pos = dic.get(jk).get(hashKey).get(size - 1);
            if ((RowDataUtil.isAccumulateMsg(record)
                            && RowDataUtil.isRetractMsg(bundle.get(jk).get(pos)))
                    || (RowDataUtil.isRetractMsg(record)
                            && RowDataUtil.isAccumulateMsg(bundle.get(jk).get(pos)))) {
                dic.get(jk).get(hashKey).remove(size - 1);
                bundle.get(jk).remove(pos);
                count--;
                if(bundle.get(jk).isEmpty()){
                    bundle.remove(jk);
                    dic.remove(jk);
                }
                return;
            }
        }
        bundle.get(jk).add(record);
        count++;
        addPos = bundle.get(jk).size() - 1;
        dic.get(jk).get(hashKey).add(addPos);
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
            foldRecord(jk, type, hashKey, record);
        } else {
            throw new RuntimeException("MiniBatch join invalid record in MiniBatchBufferNoUk");
        }
        return count;
    }

    @Override
    public List<RowData> getListRecord(RowData jk, RowData uk) {
        return bundle.get(jk);
    }

    @Override
    public Map<RowData, List<RowData>> getMapRecords() {
        return bundle;
    }
}
