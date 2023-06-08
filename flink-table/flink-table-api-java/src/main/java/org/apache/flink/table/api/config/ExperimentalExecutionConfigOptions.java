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

package org.apache.flink.table.api.config;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.configuration.ConfigOption;

import static org.apache.flink.configuration.ConfigOptions.key;

/**
 * This class holds experimental configuration constants used by Flink's table module.
 *
 * <p>NOTE: All option keys in this class must start with "table.exec".
 */
@Experimental
public class ExperimentalExecutionConfigOptions {
    public static final ConfigOption<Boolean> TABLE_EXEC_STREAM_JOIN_LESS_RECORDS_PER_KEY =
            key("table.exec.stream.join.less-records-per-key")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "When there are less records in a single key, consider using"
                                    + " ValueState to optimize the state storage.");

    public static final ConfigOption<Boolean>
            TABLE_EXEC_STREAM_JOIN_LESS_DUPLICATE_RECORDS_PER_KEY =
                    key("table.exec.stream.join.less-duplicate-records-per-key")
                            .booleanType()
                            .defaultValue(true)
                            .withDescription(
                                    "When there are less duplicate records in a single key, consider"
                                            + " using multi-bucket ValueState to optimize the state"
                                            + " storage, try not to deduplicate records.");

    public static final ConfigOption<Integer> TABLE_EXEC_STREAM_JOIN_FAST_BUCKET_SIZE =
            key("table.exec.stream.join.fast-bucket-size")
                    .intType()
                    .defaultValue(5)
                    .withDescription(
                            "Define the size of first bucket when only insert records from"
                                    + " upstream and less-duplicate-records-per-key is true.");

    public static final ConfigOption<Integer> TABLE_EXEC_STREAM_JOIN_BUCKET_SIZE =
            key("table.exec.stream.join.bucket-size")
                    .intType()
                    .defaultValue(20)
                    .withDescription(
                            "Define the size of bucket when less-records-per-key is true.");

    public static final ConfigOption<Boolean> TABLE_EXEC_STREAM_JOIN_MINI_BATCH_ENABLED =
            key("table.exec.stream.join.mini-batch-enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            String.format(
                                    "If true, the planner will create mini batch join operator if both '%s' and '%s' are configured. The default value is false.",
                                    ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED.key(),
                                    ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ALLOW_LATENCY
                                            .key()));
}
