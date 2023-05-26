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
import org.apache.flink.types.RowKind;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
     * map< Jk, map<FieldsHash,List<pos>>> FieldsHash : the hash of fields of input except the
     * RowKind. List<pos> : input corresponding position in the bundle valueList. cause there maybe
     * repetitive rows
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

    private boolean isContainNoJoinKey(RowData Jk) {
        return !bundle.containsKey(Jk);
    }

    private boolean isContainNoHashKey(RowData Jk, Integer Hk) {
        return !dic.get(Jk).containsKey(Hk);
    }

    private void addJoinKey(RowData Jk) {
        List<RowData> Val = new ArrayList<>();
        bundle.put(Jk, Val);
        Map<Integer, List<Integer>> mp = new HashMap<>();
        dic.put(Jk, mp);
    }

    private void addHashKey(RowData Jk, Integer Hk) {
        List<Integer> dic_Val = new ArrayList<>();
        dic.get(Jk).put(Hk, dic_Val);
    }

    /**
     * Fold the records only in accumulate and retract modes. The rule: the input is accumulateMsg
     * -> add to bundle the input is retractMsg -> remove the accumulateMsg with the same HashKey
     * from bundle
     *
     * <p>The same HashKey means that the input's field values are completely equivalent.
     *
     * <p>where accumulateMsg refers to +I/+U which refers to {@link RowKind#INSERT}/{@link
     * RowKind#UPDATE_AFTER}, accumulateMsg refers to -U/-D which refers to {@link
     * RowKind#UPDATE_BEFORE}/{@link RowKind#DELETE}.
     */
    private void foldRecord(RowData Jk, RowKind type, int HashKey, RowData record) {
        switch (type) {
                // accumulateMsg
            case INSERT:
            case UPDATE_AFTER:
                bundle.get(Jk).add(record);
                count++;
                int add_pos = bundle.get(Jk).size() - 1;
                dic.get(Jk).get(HashKey).add(add_pos);
                break;
                // retractMsg
            case UPDATE_BEFORE:
            case DELETE:
                int size = dic.get(Jk).get(HashKey).size();
                int pos = dic.get(Jk).get(HashKey).get(size - 1);
                dic.get(Jk).get(HashKey).remove(size - 1);
                bundle.get(Jk).remove(pos);
                count--;
                break;
        }
    }
    /** Returns false if there is no accumulateMsg before retractMsg in the same HashKey. */
    private boolean checkInvalid(RowData Jk, RowKind type, int HashKey) {
        if (type == RowKind.DELETE || type == RowKind.UPDATE_BEFORE) {
            if (isContainNoJoinKey(Jk)) {
                return false;
            }
            if (isContainNoHashKey(Jk, HashKey) || dic.get(Jk).get(HashKey).isEmpty()) {
                // no accumulateMsg before the record
                return false;
            }
        }
        return true;
    }

    /** here the Uk is null */
    @Override
    public int addRecord(RowData Jk, RowData Uk, RowData record) throws Exception {
        RowKind type = record.getRowKind();
        record.setRowKind(RowKind.INSERT);
        int HashKey = record.hashCode();
        if (checkInvalid(Jk, type, HashKey)) {
            if (isContainNoJoinKey(Jk)) {
                addJoinKey(Jk);
            }
            if (isContainNoHashKey(Jk, HashKey)) {
                addHashKey(Jk, HashKey);
            }
            foldRecord(Jk, type, HashKey, record);
            return count;
        } else {
            throw new RuntimeException("MiniBatch join err in MiniBatchBufferNoUk");
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
