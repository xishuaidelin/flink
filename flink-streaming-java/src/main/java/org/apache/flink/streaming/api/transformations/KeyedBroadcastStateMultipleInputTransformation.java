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

package org.apache.flink.streaming.api.transformations;

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 *  For multi-input with broadcast state. Especially for multi-join with broadcast input.
 *  Especially, num of non-broadcast inputs <=2, num of broadcast inputs >=1.
 *
 * */
public class KeyedBroadcastStateMultipleInputTransformation<OUT>  extends KeyedMultipleInputTransformation<OUT>{



    // num of nonBroadcastInputs
    private final int numNonBcInputs;

    public KeyedBroadcastStateMultipleInputTransformation(
            String name,
            StreamOperatorFactory<OUT> operatorFactory,
            TypeInformation<OUT> outputType,
            int parallelism,
            TypeInformation<?> stateKeyType,
            int numNonBcInputs) {
        super(name, operatorFactory, outputType, parallelism, stateKeyType);
        this.numNonBcInputs = numNonBcInputs;
    }

//    public List<Transformation<?>> getNonBroadcastInputs() {
//        if( numNonBcInputs == 1 ){
//            return Collections.singletonList(
//                    inputs.get(0));
//        }
//        return  Arrays.asList(
//                inputs.get(0),
//                inputs.get(1));
//
//    }

}
