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

package org.apache.flink.table.runtime.operators.join.stream.bundle;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.join.stream.state.JoinInputSideSpec;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A {@link BufferBundle} is a bundle to buffer the input records in memory and fold data based on
 * specified pattern to reduce state access. The bundle is used in MiniBatchStreamingJoinOperator.
 * The structure of the bundle varies depending on the {@link JoinInputSideSpec}.
 */
@Internal
public interface BufferBundle {

    /**
     * Adds a record into the bufferBundle when processing element in a stream and this function
     * would return the size of the bufferBundle.
     *
     * @param joinKey the joinKey associated with the record.
     * @param uniqueKey the uniqueKey associated with the record. This could be null.
     * @param record The record to add.
     * @return number of processed by current bundle.
     */
    int addRecord(RowData joinKey, @Nullable RowData uniqueKey, RowData record);

    /**
     * Get the joinKeys in bufferBundle. Whether to override this method is based on the
     * implementing class.
     */
    default Set<RowData> getJoinKeys() {
        return Collections.emptySet();
    }

    /**
     * Get records associated with joinKeys from bufferBundle.
     *
     * @return a map whose key is joinKey and value is list of records.
     */
    Map<RowData, List<RowData>> getRecords() throws Exception;

    /**
     * Get records associated with joinKeys from bufferBundle. And this function is different from
     * getRecords() above where getRecords() returns a map whose key is joinKey and value is list of
     * records.
     *
     * @param joinKey one of joinKeys stored in this bundle.
     * @return a map whose key is uniqueKey and value is a list of records.
     */
    Map<RowData, List<RowData>> getRecordsWithJoinKey(RowData joinKey) throws Exception;

    /** Check if this bufferBundle is empty. */
    boolean isEmpty();

    /** Clear this bufferBundle. */
    void clear();
}
