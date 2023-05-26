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

package org.apache.flink.table.runtime.operators.join.stream;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.config.ExperimentalExecutionConfigOptions;

import java.io.Serializable;

/** Define config options for streaming join. */
public class StreamingJoinConfig implements Serializable {

    private final boolean lessRecordsPerKey;
    private final boolean lessDuplicateRecordsPerKey;
    private final int fastBucketSize;
    private final int bucketSize;
    //    private final ExecutionConfigOptions.KvSeparate kvSeparate;

    private StreamingJoinConfig(
            boolean lessRecordsPerKey,
            boolean lessDuplicateRecordsPerKey,
            int fastBucketSize,
            int bucketSize
            //            ExecutionConfigOptions.KvSeparate kvSeparate) {
            ) {
        this.lessRecordsPerKey = lessRecordsPerKey;
        this.lessDuplicateRecordsPerKey = lessDuplicateRecordsPerKey;
        this.fastBucketSize = fastBucketSize;
        this.bucketSize = bucketSize;
        //        this.kvSeparate = kvSeparate;
    }

    public static StreamingJoinConfig fromTableConfig(ReadableConfig config) {
        return new StreamingJoinConfig(
                config.get(
                        ExperimentalExecutionConfigOptions
                                .TABLE_EXEC_STREAM_JOIN_LESS_RECORDS_PER_KEY),
                config.get(
                        ExperimentalExecutionConfigOptions
                                .TABLE_EXEC_STREAM_JOIN_LESS_DUPLICATE_RECORDS_PER_KEY),
                config.get(
                        ExperimentalExecutionConfigOptions.TABLE_EXEC_STREAM_JOIN_FAST_BUCKET_SIZE),
                config.get(ExperimentalExecutionConfigOptions.TABLE_EXEC_STREAM_JOIN_BUCKET_SIZE)
                //                config.get(ExecutionConfigOptions.TABLE_EXEC_JOIN_KV_SEPARATE)
                );
    }

    public boolean isLessRecordsPerKey() {
        return lessRecordsPerKey;
    }

    public boolean isLessDuplicateRecordsPerKey() {
        return lessDuplicateRecordsPerKey;
    }

    public int getFastBucketSize() {
        return fastBucketSize;
    }

    public int getBucketSize() {
        return bucketSize;
    }

    //    public ExecutionConfigOptions.KvSeparate getKvSeparate() {
    //        return kvSeparate;
    //    }
}
