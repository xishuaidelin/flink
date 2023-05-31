/// *
// * Licensed to the Apache Software Foundation (ASF) under one
// * or more contributor license agreements.  See the NOTICE file
// * distributed with this work for additional information
// * regarding copyright ownership.  The ASF licenses this file
// * to you under the Apache License, Version 2.0 (the
// * "License"); you may not use this file except in compliance
// * with the License.  You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
// package org.apache.flink.table.runtime.operators.join.stream;
//
// import org.apache.flink.table.runtime.operators.join.stream.state.JoinInputSideSpec;
// import org.apache.flink.table.types.logical.LogicalType;
// import org.apache.flink.table.types.logical.RowType;
// import org.apache.flink.table.utils.HandwrittenSelectorUtil;
//
// import org.junit.jupiter.api.TestInfo;
//
// import static
// org.apache.flink.table.runtime.operators.join.stream.StreamingJoinOperatorTestBase.STATE_RETENTION_TIME_EXTRACTOR;
//
// public final class StreamingMiniBatchJoinOperatorTest
//        extends StreamingMiniBatchJoinOperatorTestbase {
//
//    public StreamingMiniBatchJoinOperatorTest(boolean JkUk, boolean HasUk) {
//        if (JkUk) {
//            leftInputSpec =
//                    JoinInputSideSpec.withUniqueKeyContainedByJoinKey(
//                            leftTypeInfo, leftKeySelector);
//            rightInputSpec =
//                    JoinInputSideSpec.withUniqueKeyContainedByJoinKey(
//                            rightTypeInfo, rightKeySelector);
//
//            leftKeySelector =
//                    HandwrittenSelectorUtil.getRowDataSelector(
//                            new int[] {1},
//                            leftTypeInfo.toRowType().getChildren().toArray(new LogicalType[0]));
//            rightKeySelector =
//                    HandwrittenSelectorUtil.getRowDataSelector(
//                            new int[] {0},
//                            rightTypeInfo.toRowType().getChildren().toArray(new LogicalType[0]));
//        } else if (HasUk) {
//            leftInputSpec =
//                    JoinInputSideSpec.withUniqueKeyContainedByJoinKey(
//                            leftTypeInfo, leftKeySelector);
//            rightInputSpec =
//                    JoinInputSideSpec.withUniqueKeyContainedByJoinKey(
//                            rightTypeInfo, rightKeySelector);
//
//            leftKeySelector =
//                    HandwrittenSelectorUtil.getRowDataSelector(
//                            new int[] {1},
//                            leftTypeInfo.toRowType().getChildren().toArray(new LogicalType[0]));
//            rightKeySelector =
//                    HandwrittenSelectorUtil.getRowDataSelector(
//                            new int[] {0},
//                            rightTypeInfo.toRowType().getChildren().toArray(new LogicalType[0]));
//        } else {
//            leftInputSpec =
//                    JoinInputSideSpec.withUniqueKeyContainedByJoinKey(
//                            leftTypeInfo, leftKeySelector);
//            rightInputSpec =
//                    JoinInputSideSpec.withUniqueKeyContainedByJoinKey(
//                            rightTypeInfo, rightKeySelector);
//
//            leftKeySelector =
//                    HandwrittenSelectorUtil.getRowDataSelector(
//                            new int[] {1},
//                            leftTypeInfo.toRowType().getChildren().toArray(new LogicalType[0]));
//            rightKeySelector =
//                    HandwrittenSelectorUtil.getRowDataSelector(
//                            new int[] {0},
//                            rightTypeInfo.toRowType().getChildren().toArray(new LogicalType[0]));
//        }
//    }
//
//    @Override
//    protected MiniBatchStreamingJoinOperator createJoinOperator(TestInfo testInfo) {
////        Boolean[] joinTypeSpec = JOIN_TYPE_EXTRACTOR.apply(testInfo.getDisplayName());
////        Long[] ttl = STATE_RETENTION_TIME_EXTRACTOR.apply(testInfo.getTags());
////        return MiniBatchStreamingJoinOperator.newMiniBatchStreamJoinOperator(
////                leftTypeInfo,
////                rightTypeInfo,
////                joinCondition,
////                leftInputSpec,
////                rightInputSpec,
////                joinTypeSpec[0],
////                joinTypeSpec[1],
////                new boolean[] {true},
////                ttl[0],
////                ttl[1]);
////
////        return MiniBatchStreamingJoinOperator.newMiniBatchStreamJoinOperator(
////                joinType, // inner / left / right / full
////                leftTypeInfo,
////                rightTypeInfo,
////                joinCondition,
////                leftInputSpec,
////                rightInputSpec,
////                leftIsOuter,
////                rightIsOuter,
////                joinSpec.getFilterNulls(),
////                minRetentionTime,
////                JoinUtil.createMiniBatchTrigger(config));
//    }
//
//    /**
//     * Create streaming join operator according to {@link TestInfo}.
//     *
//     * @param testInfo
//     */
//    @Override
//    protected AbstractStreamingJoinOperator createJoinOperator(TestInfo testInfo) {
//        return null;
//    }
//
//    /** Get the output row type of join operator. */
//    @Override
//    protected RowType getOutputType() {
//        return null;
//    }
// }
