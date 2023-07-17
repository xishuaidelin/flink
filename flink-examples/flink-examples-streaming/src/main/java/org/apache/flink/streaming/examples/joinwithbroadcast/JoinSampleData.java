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

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.examples.utils.ThrottledIterator;

import org.apache.commons.lang3.RandomStringUtils;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Random;

/** For test. */
public class JoinSampleData {
    //    static final String[] NAMES = {"tom", "jerry", "alice", "bob", "john", "grace"};
    //    static final int GRADE_COUNT = 5;
    //    static final int SALARY_MAX = 10000;

    /** Continuously generates (name, grade). */
    public static class GradeSource
            implements Iterator<Tuple3<String, String, String>>, Serializable {

        private final Random rnd = new Random(hashCode());

        @Override
        public boolean hasNext() {
            return true;
        }

        @Override
        public Tuple3<String, String, String> next() {
            //            return new Tuple2<>(NAMES[rnd.nextInt(NAMES.length)],
            // rnd.nextInt(GRADE_COUNT) + 1);
            return new Tuple3<>(
                    RandomStringUtils.randomAlphanumeric(256),
                    RandomStringUtils.randomAlphanumeric(2048),
                    RandomStringUtils.randomAlphanumeric(2048));
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        public static DataStream<Tuple3<String, String, String>> getSource(
                StreamExecutionEnvironment env, long rate) {
            return env.fromCollection(
                    new ThrottledIterator<>(new JoinSampleData.GradeSource(), rate),
                    TypeInformation.of(new TypeHint<Tuple3<String, String, String>>() {}));
        }
    }

    /** Continuously generates (name, salary). */
    public static class SalarySource
            implements Iterator<Tuple3<String, String, String>>, Serializable {

        private final Random rnd = new Random(hashCode());

        @Override
        public boolean hasNext() {
            return true;
        }

        @Override
        public Tuple3<String, String, String> next() {
            // return new Tuple2<>(NAMES[rnd.nextInt(NAMES.length)], rnd.nextInt(SALARY_MAX) + 1);
            return new Tuple3<>(
                    // 生成指定length的随机字符串（A-Z，a-z，0-9）
                    RandomStringUtils.randomAlphanumeric(256),
                    RandomStringUtils.randomAlphanumeric(2048),
                    RandomStringUtils.randomAlphanumeric(2048));
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        public static DataStream<Tuple3<String, String, String>> getSource(
                StreamExecutionEnvironment env, long rate) {
            return env.fromCollection(
                    new ThrottledIterator<>(new JoinSampleData.SalarySource(), rate),
                    TypeInformation.of(new TypeHint<Tuple3<String, String, String>>() {}));
        }
    }

    //    private static String generateRandomString(int length) {
    //        Random random = new Random();
    //        StringBuilder sb = new StringBuilder(length);
    //        for (int i = 0; i < length; i++) {
    //            int randomInt = random.nextInt(26) + 97; // 生成 a ~ z 之间的随机整数
    //            char randomChar = (char) randomInt;
    //            sb.append(randomChar);
    //        }
    //        return sb.toString();
    //    }
}
