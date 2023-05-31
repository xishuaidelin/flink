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
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.join.stream.state.JoinInputSideSpec;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.utils.HandwrittenSelectorUtil;

/** TestBase for BatchBufferTest. */
public abstract class BatchBufferTestBase {
    /**
     * for JkContainsUk -> JoinKey & UniqueKey = order_id. for ContainsUk -> JoinKey = order_id &
     * UniqueKey = line_order_id. for NoUk -> JoinKey = order_id.
     */
    protected final InternalTypeInfo<RowData> inputTypeInfo =
            InternalTypeInfo.of(
                    RowType.of(
                            new LogicalType[] {
                                new CharType(false, 20),
                                new CharType(false, 20),
                                VarCharType.STRING_TYPE
                            },
                            new String[] {"order_id", "line_order_id", "shipping_address"}));

    protected final RowDataKeySelector inputKeySelector1 =
            HandwrittenSelectorUtil.getRowDataSelector(
                    new int[] {0},
                    inputTypeInfo.toRowType().getChildren().toArray(new LogicalType[0]));

    protected final RowDataKeySelector inputKeySelector2 =
            HandwrittenSelectorUtil.getRowDataSelector(
                    new int[] {1},
                    inputTypeInfo.toRowType().getChildren().toArray(new LogicalType[0]));

    protected final JoinInputSideSpec inputSpecJkUk =
            JoinInputSideSpec.withUniqueKeyContainedByJoinKey(inputTypeInfo, inputKeySelector1);

    protected final JoinInputSideSpec inputSpecHasUk =
            JoinInputSideSpec.withUniqueKey(inputTypeInfo, inputKeySelector2);
}
