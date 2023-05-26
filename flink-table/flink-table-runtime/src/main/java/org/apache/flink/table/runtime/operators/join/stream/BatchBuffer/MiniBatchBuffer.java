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

import java.util.List;
import java.util.Map;

// here create a new class to represent the buffer with three cases
// 1. JoinKeyContainsUniqueKey 2.InputSideHasUniqueKey 3.InputSideHasNoUniqueKey

// 1. buffer ->  Map<JoinKey, Input>
// 2. buffer ->  Map<JoinKey, Map<UniqueKey,Input>>
// 3. buffer ->  Map<JoinKey, Input>

public interface MiniBatchBuffer {
    public boolean isEmpty();

    public void clear();

    public int size();

    /** return the size of the buffer now */
    public int addRecord(RowData Jk, RowData Uk, RowData record) throws Exception;

    public List<RowData> getListRecord(RowData Jk, RowData Uk);

    /** return the Map<RowData(joinKey),List<RowData>> */
    public Map<RowData, List<RowData>> getMapRecords();
}
