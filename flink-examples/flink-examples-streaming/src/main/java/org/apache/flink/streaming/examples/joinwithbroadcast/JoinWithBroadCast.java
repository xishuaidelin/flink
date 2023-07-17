/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.examples.joinwithbroadcast;

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Example illustrating a stream join between two data streams.
 *
 * <p>The example works on two input streams with pairs (name, grade) and (name, salary)
 * respectively. It joins the streams based on "name".
 *
 * <p>The example uses a built-in sample data generator that generates the streams of pairs at a
 * configurable rate.
 */
public class JoinWithBroadCast {
    public static void main(String[] args) throws Exception {

        // parse the parameters
        final ParameterTool params = ParameterTool.fromArgs(args);
        final long rate = params.getLong("rate", 256L);

        System.out.println("Using data rate=" + rate);
        System.out.println(
                "To customize example, use: JoinWithBroadCast [--rate <elements-per-second>]");

        // obtain execution environment, run this example in "ingestion time"
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        // create the data sources for both grades and salaries
        DataStream<Tuple3<String, String, String>> grades =
                JoinSampleData.GradeSource.getSource(env, rate);

        DataStream<Tuple3<String, String, String>> salaries =
                JoinSampleData.SalarySource.getSource(env, rate);

        // run the actual window join program
        // for testability, this functionality is in a separate method.
        DataStream<Tuple5<String, String, String, String, String>> joinedStream =
                runBroadcastJoin(grades, salaries);

        // print the results with a single thread, rather than in parallel
        joinedStream.print().setParallelism(1);

        // execute program
        env.execute("BroadcastJoinExample");
    }

    public static DataStream<Tuple5<String, String, String, String, String>> runBroadcastJoin(
            DataStream<Tuple3<String, String, String>> grades,
            DataStream<Tuple3<String, String, String>> salaries) {

        MapStateDescriptor<String, Tuple3<String, String, String>> bcStateDescriptor =
                new MapStateDescriptor<>(
                        "name_salary",
                        Types.STRING,
                        TypeInformation.of(new TypeHint<Tuple3<String, String, String>>() {}));

        return grades.connect(salaries.broadcast(bcStateDescriptor))
                .process(
                        new BroadcastProcessFunction<
                                Tuple3<String, String, String>,
                                Tuple3<String, String, String>,
                                Tuple5<String, String, String, String, String>>() {
                            // pattern match
                            @Override
                            public void processBroadcastElement(
                                    Tuple3<String, String, String> value,
                                    Context ctx,
                                    Collector<Tuple5<String, String, String, String, String>> out)
                                    throws Exception {
                                ctx.getBroadcastState(bcStateDescriptor).put(value.f0, value);
                            }

                            @Override
                            public void processElement(
                                    Tuple3<String, String, String> value,
                                    ReadOnlyContext ctx,
                                    Collector<Tuple5<String, String, String, String, String>> out)
                                    throws Exception {
                                Tuple3<String, String, String> bcVal =
                                        ctx.getBroadcastState(bcStateDescriptor).get(value.f0);
                                if (bcVal != null) {
                                    out.collect(
                                            new Tuple5<>(
                                                    bcVal.f0, bcVal.f1, bcVal.f2, value.f1,
                                                    value.f2));
                                }
                            }
                        });
    }
}
