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

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.join.stream.state.JoinInputSideSpec;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

import org.junit.jupiter.api.TestInfo;

/** This class would offer the base for testing of JoinKeyContainsUk / InputHasUk / InputHasNoUk. */
public abstract class StreamingMiniBatchJoinOperatorTestbase {
    protected final InternalTypeInfo<RowData> leftTypeInfo =
            InternalTypeInfo.of(
                    RowType.of(
                            new LogicalType[] {
                                new CharType(false, 20),
                                new CharType(false, 20),
                                VarCharType.STRING_TYPE
                            },
                            new String[] {"order_id", "line_order_id", "shipping_address"}));

    protected final InternalTypeInfo<RowData> rightTypeInfo =
            InternalTypeInfo.of(
                    RowType.of(
                            new LogicalType[] {new CharType(false, 20), new CharType(true, 10)},
                            new String[] {"line_order_id0", "line_order_ship_mode"}));

    protected RowDataKeySelector leftKeySelector;
    protected RowDataKeySelector rightKeySelector;

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

    protected abstract AbstractStreamingJoinOperator createJoinOperator(TestInfo testInfo);

    protected abstract RowType getOutputType();
}
