
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

package org.apache.flink.table.data.utils;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RawValueData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.types.RowKind;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Objects;

/**
 * An implementation of {@link RowData} which is backed by multiple concatenated {@link RowData}.
 *
 * <p>This implementation is mutable to allow for performant changes in hot code paths.
 */
@PublicEvolving
public class MultiJoinedRowData implements RowData {

    // default rowKind with Insert
    private RowKind rowKind = RowKind.INSERT;

    private RowData[] rows;

    private int numRows;

    /**
     * Creates a new {@link MultiJoinedRowData} of kind {@link RowKind#INSERT}, but without backing rows.
     *
     * <p>Note that it must be ensured that the backing rows are set to non-{@code null} values
     * before accessing data from this {@link MultiJoinedRowData}.
     */
    public MultiJoinedRowData() {}

    /**
     * Creates a new {@link MultiJoinedRowData} of kind {@link RowKind#INSERT} backed by {@param rows}
     *
     * <p>Note that it must be ensured that the backing rows are set to non-{@code null} values
     * before accessing data from this {@link MultiJoinedRowData}.
     */
    public MultiJoinedRowData(@Nullable RowData[] rows, int num) {
        this(RowKind.INSERT, rows, num);
    }

    /**
     * Creates a new {@link MultiJoinedRowData} of kind {@param rowKind} backed by {@param rows}
     *
     * <p>Note that it must be ensured that the backing rows are set to non-{@code null} values
     * before accessing data from this {@link MultiJoinedRowData}.
     */
    public MultiJoinedRowData(RowKind rowKind, @Nullable RowData[] rows, int num) {
        this.rowKind = rowKind;
        this.rows = rows;
        this.numRows = num;
    }

    /**
     * Replaces the {@link RowData} backing this {@link MultiJoinedRowData}.
     *
     * <p>This method replaces the backing rows in place and does not return a new object. This is
     * done for performance reasons.
     */
    public MultiJoinedRowData replace(RowData[] rows) {
        this.rows = rows;
        return this;
    }

    // ---------------------------------------------------------------------------------------------

    public int getNumRows() {
        return numRows;
    }

    @Override
    public int getArity() {

        int numFields = 0;

        for(RowData row : rows){
            numFields += row.getArity();
        }

        return numFields;
    }

    @Override
    public RowKind getRowKind() {
        return rowKind;
    }

    @Override
    public void setRowKind(RowKind kind) {
        this.rowKind = kind;
    }

    @Override
    public boolean isNullAt(int pos) {
        Tuple2<RowData, Integer> tuple = getProperRow(pos);

        return tuple.f0.isNullAt(tuple.f1);
    }

    @Override
    public boolean getBoolean(int pos) {
        Tuple2<RowData, Integer> tuple = getProperRow(pos);

        return tuple.f0.getBoolean(tuple.f1);
    }

    @Override
    public byte getByte(int pos) {
        Tuple2<RowData, Integer> tuple = getProperRow(pos);

        return tuple.f0.getByte(tuple.f1);
    }

    @Override
    public short getShort(int pos) {
        Tuple2<RowData, Integer> tuple = getProperRow(pos);

        return tuple.f0.getShort(tuple.f1);
    }

    @Override
    public int getInt(int pos) {
        Tuple2<RowData, Integer> tuple = getProperRow(pos);

        return tuple.f0.getInt(tuple.f1);
    }

    @Override
    public long getLong(int pos) {
        Tuple2<RowData, Integer> tuple = getProperRow(pos);

        return tuple.f0.getLong(tuple.f1);
    }

    @Override
    public float getFloat(int pos) {
        Tuple2<RowData, Integer> tuple = getProperRow(pos);

        return tuple.f0.getFloat(tuple.f1);
    }

    @Override
    public double getDouble(int pos) {
        Tuple2<RowData, Integer> tuple = getProperRow(pos);

        return tuple.f0.getDouble(tuple.f1);
    }

    @Override
    public StringData getString(int pos) {
        Tuple2<RowData, Integer> tuple = getProperRow(pos);

        return tuple.f0.getString(tuple.f1);
    }

    @Override
    public DecimalData getDecimal(int pos, int precision, int scale) {
        Tuple2<RowData, Integer> tuple = getProperRow(pos);

        return tuple.f0.getDecimal(tuple.f1, precision, scale);
    }

    @Override
    public TimestampData getTimestamp(int pos, int precision) {
        Tuple2<RowData, Integer> tuple = getProperRow(pos);

        return tuple.f0.getTimestamp(tuple.f1, precision);
    }

    @Override
    public <T> RawValueData<T> getRawValue(int pos) {
        Tuple2<RowData, Integer> tuple = getProperRow(pos);

        return tuple.f0.getRawValue(tuple.f1);
    }

    @Override
    public byte[] getBinary(int pos) {
        Tuple2<RowData, Integer> tuple = getProperRow(pos);

        return tuple.f0.getBinary(tuple.f1);
    }

    @Override
    public ArrayData getArray(int pos) {
        Tuple2<RowData, Integer> tuple = getProperRow(pos);

        return tuple.f0.getArray(tuple.f1);
    }

    @Override
    public MapData getMap(int pos) {
        Tuple2<RowData, Integer> tuple = getProperRow(pos);

        return tuple.f0.getMap(tuple.f1);
    }

    @Override
    public RowData getRow(int pos, int numFields) {
        Tuple2<RowData, Integer> tuple = getProperRow(pos);

        return tuple.f0.getRow(tuple.f1,numFields);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        return equalsRow(o);
    }

    @Override
    public int hashCode() {
        return Objects.hash(rowKind, Arrays.hashCode(rows), numRows);
    }

    @Override
    public String toString() {
        String rowStr = "";
        for(int i = 0; i < numRows; i++){
            if ( i > 0 ) {
                rowStr += ",";
            }
            rowStr += "row"+(i+1)+ "=" + rows[i];
        }

        return rowStr;
    }

    private boolean equalsRow(Object o) {
        MultiJoinedRowData that = (MultiJoinedRowData) o;

        if( Objects.equals(rowKind, that.rowKind)
                && Objects.equals(numRows, that.numRows) ){
            for(int i = 0; i < numRows; i++){
                if (!Objects.equals(rows[i], that.rows[i])){
                    return false;
                }
            }
        }else{
            return false;
        }
        return true;
    }


    private Tuple2<RowData,Integer> getProperRow(int pos){
        int accNum = 0;

        for (int i = 0; i < numRows; i++){
            if( pos >= accNum + rows[i].getArity() ){
                accNum += rows[i].getArity();
                continue;
            }
            return new Tuple2<>(rows[i], pos-accNum);
        }

        return null;
    }
}
