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

import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.join.stream.state.JoinInputSideSpec;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.util.RowDataHarnessAssertor;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.utils.HandwrittenSelectorUtil;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;

import java.util.Set;
import java.util.function.Function;

/** This class would offer the base for testing of JoinKeyContainsUk / InputHasUk / InputHasNoUk. */
public abstract class StreamingMiniBatchJoinOperatorTestbase {
    protected static final InternalTypeInfo<RowData> leftTypeInfo =
            InternalTypeInfo.of(
                    RowType.of(
                            new LogicalType[] {
                                new CharType(false, 20),
                                new CharType(false, 20),
                                VarCharType.STRING_TYPE
                            },
                            new String[] {"order_id", "line_order_id", "shipping_address"}));

    protected static final InternalTypeInfo<RowData> rightTypeInfo =
            InternalTypeInfo.of(
                    RowType.of(
                            new LogicalType[] {
                                new CharType(false, 20),
                                new CharType(false, 20),
                                new CharType(true, 10)
                            },
                            new String[] {"order_id#", "line_order_id0", "line_order_ship_mode"}));

    protected static RowDataKeySelector joinKeySelector =
            HandwrittenSelectorUtil.getRowDataSelector(
                    new int[] {1},
                    leftTypeInfo.toRowType().getChildren().toArray(new LogicalType[0]));

    protected JoinInputSideSpec leftInputSpec;

    protected JoinInputSideSpec rightInputSpec;

    protected final InternalTypeInfo<RowData> joinKeyTypeInfo =
            InternalTypeInfo.of(new CharType(false, 20));

    protected final String funcCode =
            "public class ConditionFunction extends org.apache.flink.api.common.functions.AbstractRichFunction "
                    + "implements org.apache.flink.table.runtime.generated.JoinCondition {\n"
                    + "\n"
                    + "    public ConditionFunction(Object[] reference) {\n"
                    + "    }\n"
                    + "\n"
                    + "    @Override\n"
                    + "    public boolean apply(org.apache.flink.table.data.RowData in1, org.apache.flink.table.data.RowData in2) {\n"
                    + "        return true;\n"
                    + "    }\n"
                    + "\n"
                    + "    @Override\n"
                    + "    public void close() throws Exception {\n"
                    + "        super.close();\n"
                    + "    }"
                    + "}\n";
    protected final GeneratedJoinCondition joinCondition =
            new GeneratedJoinCondition("ConditionFunction", funcCode, new Object[0]);

    protected final RowDataHarnessAssertor assertor =
            new RowDataHarnessAssertor(getOutputType().getChildren().toArray(new LogicalType[0]));

    protected KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData>
            testHarness;

    @BeforeEach
    public void beforeEach(TestInfo testInfo) throws Exception {
        testHarness =
                new KeyedTwoInputStreamOperatorTestHarness<>(
                        createJoinOperator(testInfo),
                        joinKeySelector,
                        joinKeySelector,
                        joinKeyTypeInfo);
        testHarness.open();
    }

    @AfterEach
    public void afterEach() throws Exception {
        testHarness.close();
    }

    protected static final Function<String, RowDataKeySelector[]> UniqueKeySelector_EXTRACTOR =
            (testDisplayName) -> {
                if (testDisplayName.contains("JkUk")) {
                    return new RowDataKeySelector[] {
                        HandwrittenSelectorUtil.getRowDataSelector(
                                new int[] {1},
                                leftTypeInfo.toRowType().getChildren().toArray(new LogicalType[0])),
                        HandwrittenSelectorUtil.getRowDataSelector(
                                new int[] {1},
                                rightTypeInfo.toRowType().getChildren().toArray(new LogicalType[0]))
                    };
                } else if (testDisplayName.contains("HasUk")) {
                    return new RowDataKeySelector[] {
                        HandwrittenSelectorUtil.getRowDataSelector(
                                new int[] {0},
                                leftTypeInfo.toRowType().getChildren().toArray(new LogicalType[0])),
                        HandwrittenSelectorUtil.getRowDataSelector(
                                new int[] {0},
                                rightTypeInfo.toRowType().getChildren().toArray(new LogicalType[0]))
                    };
                } else {
                    return new RowDataKeySelector[] {null, null};
                }
            };



    protected static final Function<Set<String>, Integer> MINIBATCH_SIZE_EXTRACTOR =
            (tags) -> {
                int size = 5;
                if (tags.isEmpty()) {
                    return size; // default
                }
                for (String tag : tags) {
                    String[] splits = tag.split("=");
                    int value = Integer.parseInt(splits[1].trim());
                    if (splits[0].trim().startsWith("miniBatchSize")) {
                        size = value;
                        break;
                    }
                }
                return size;
            };

    protected static final Function<Set<String>, Long[]> STATE_RETENTION_TIME_EXTRACTOR =
            (tags) -> {
                Long[] ttl = new Long[] {0L, 0L};
                for (String tag : tags) {
                    String[] splits = tag.split("=");
                    long value = Long.parseLong(splits[1].trim());
                    if (splits[0].trim().startsWith("left")) {
                        ttl[0] = value;
                    } else {
                        ttl[1] = value;
                    }
                }
                return ttl;
            };

    protected abstract AbstractStreamingJoinOperator createJoinOperator(TestInfo testInfo);

    protected abstract RowType getOutputType();
}
