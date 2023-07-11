/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.examples.broadcaststate;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

/** For testing how big could the broadcast state be. */
public class BroadcastStateSample {

    static StreamExecutionEnvironment env;

    static GeneratorFunction<Long, Action> generatorActionFunction;
    static GeneratorFunction<Long, Pattern> generatorPatternFunction;
    static DataStreamSource<Pattern> sourcePatternStream;
    static DataStreamSource<Action> sourceActionStream;

    static DataStream<Tuple2<Long, Pattern>> matches;

    /** Define the action. */
    public static class Action {
        public long userId;

        public String action;

        public Action() {}

        public Action(long id, String act) {
            userId = id;
            action = act;
        }

        @Override
        public String toString() {
            return userId + " : " + action;
        }
    }

    /** mock the patterns. */
    public static class Pattern {
        public String firstAction;

        public String secondAction;

        public Pattern() {}

        public Pattern(String act1, String act2) {
            firstAction = act1;
            secondAction = act2;
        }

        @Override
        public String toString() {
            return firstAction + " : " + secondAction;
        }
    }

    /** Define the evaluator. */
    public static class PatternEvaluator
            extends KeyedBroadcastProcessFunction<Long, Action, Pattern, Tuple2<Long, Pattern>> {
        // handle for keyed state (per user)
        ValueState<String> prevActionState;
        // broadcast state descriptor
        MapStateDescriptor<String, Pattern> patternDesc;

        @Override
        public void open(Configuration conf) {
            // initialize keyed state
            prevActionState =
                    getRuntimeContext()
                            .getState(new ValueStateDescriptor<>("lastAction", Types.STRING));
            patternDesc =
                    new MapStateDescriptor<>("patterns", Types.STRING, Types.POJO(Pattern.class));
        }

        /**
         * Called for each user action. Evaluates the current pattern against the previous and
         * current action of the user.
         */
        @Override
        public void processElement(
                Action action, ReadOnlyContext ctx, Collector<Tuple2<Long, Pattern>> out)
                throws Exception {
            // get current pattern from broadcast state
            Pattern pattern =
                    ctx.getBroadcastState(this.patternDesc)
                            // access MapState with specified id
                            .get(action.action);
            // get previous action of current user from keyed state
            String prevAction = prevActionState.value();
            if (pattern != null && prevAction != null) {
                // user had an action before, check if pattern matches
                if (pattern.firstAction.equals(prevAction)
                        && pattern.secondAction.equals(action.action)) {
                    // MATCH
                    out.collect(new Tuple2<>(ctx.getCurrentKey(), pattern));
                }
            }
            // update keyed state and remember action for next pattern evaluation
            prevActionState.update(action.action);
        }

        /** Called for each new pattern. Overwrites the current pattern with the new pattern. */
        @Override
        public void processBroadcastElement(
                Pattern pattern, Context ctx, Collector<Tuple2<Long, Pattern>> out)
                throws Exception {
            // store the new pattern by updating the broadcast state
            BroadcastState<String, Pattern> bcState = ctx.getBroadcastState(patternDesc);
            // storing in MapState with specified id
            bcState.put(pattern.firstAction, pattern);
        }
    }

    // register data generation source
    public static void open() {

        // create environments of both APIs
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        //        tableEnv = StreamTableEnvironment.create(env);

        // create a data generation source
        generatorActionFunction = index -> new Action(index, "Action" + index);

        generatorPatternFunction = index -> new Pattern("Action" + index, "Action" + index);

        DataGeneratorSource<Action> sourceAction =
                new DataGeneratorSource<>(
                        generatorActionFunction, 100000000, Types.POJO(Action.class));

        sourceActionStream =
                env.fromSource(
                        sourceAction, WatermarkStrategy.noWatermarks(), "Generator Source Action");

        DataGeneratorSource<Pattern> sourcePattern =
                new DataGeneratorSource<>(
                        generatorPatternFunction, 100000000, Types.POJO(Pattern.class));

        sourcePatternStream =
                env.fromSource(
                        sourcePattern,
                        WatermarkStrategy.noWatermarks(),
                        "Generator Source Pattern");
    }

    public static void main(String[] args) throws Exception {

        open();
        MapStateDescriptor<Void, Pattern> bcStateDescriptor =
                new MapStateDescriptor<>("patterns", Types.VOID, Types.POJO(Pattern.class));

        BroadcastStream<Pattern> bcPatterns = sourcePatternStream.broadcast(bcStateDescriptor);

        KeyedStream<Action, Long> actionsByUser =
                sourceActionStream.keyBy((KeySelector<Action, Long>) action -> action.userId);

        matches = actionsByUser.connect(bcPatterns).process(new PatternEvaluator());

        matches.print();

        env.execute();
    }
}
