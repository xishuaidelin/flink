package org.apache.flink.table.planner.plan.nodes.exec.spec;

import org.apache.calcite.rex.RexNode;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;

/**
 * MultiJoinSpec describes how multiple tables will be joined.
 * Below is the logic of join with two streams.
 *
 * JoinSpec describes how two tables will be joined.
 *
 * <p>This class corresponds to {@link org.apache.calcite.rel.core.Join} rel node.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class MultiJoinSpec {
    public static final String FIELD_NAME_JOIN_TYPE = "joinType";
    public static final String FIELD_NAME_LEFT_KEYS = "leftKeys";
    public static final String FIELD_NAME_RIGHT_KEYS = "rightKeys";
    public static final String FIELD_NAME_FILTER_NULLS = "filterNulls";
    public static final String FIELD_NAME_NON_EQUI_CONDITION = "nonEquiCondition";

    /** {@link FlinkJoinType} of the join. */
    @JsonProperty(FIELD_NAME_JOIN_TYPE)
    private final FlinkJoinType joinType;

    /** 0-based index of join keys in left side. */
    @JsonProperty(FIELD_NAME_LEFT_KEYS)
    private final int[] leftKeys;

    /** 0-based index of join keys in right side. */
    @JsonProperty(FIELD_NAME_RIGHT_KEYS)
    private final int[] rightKeys;

    /** whether to filter null values or not for each corresponding index join key. */
    @JsonProperty(FIELD_NAME_FILTER_NULLS)
    private final boolean[] filterNulls;

    /** Non Equi join conditions. */
    @JsonProperty(FIELD_NAME_NON_EQUI_CONDITION)
    private final @Nullable RexNode nonEquiCondition;

    @JsonCreator
    public MultiJoinSpec(
            @JsonProperty(FIELD_NAME_JOIN_TYPE) FlinkJoinType joinType,
            @JsonProperty(FIELD_NAME_LEFT_KEYS) int[] leftKeys,
            @JsonProperty(FIELD_NAME_RIGHT_KEYS) int[] rightKeys,
            @JsonProperty(FIELD_NAME_FILTER_NULLS) boolean[] filterNulls,
            @JsonProperty(FIELD_NAME_NON_EQUI_CONDITION) @Nullable RexNode nonEquiCondition) {
        this.joinType = Preconditions.checkNotNull(joinType);
        this.leftKeys = Preconditions.checkNotNull(leftKeys);
        this.rightKeys = Preconditions.checkNotNull(rightKeys);
        this.filterNulls = Preconditions.checkNotNull(filterNulls);
        Preconditions.checkArgument(leftKeys.length == rightKeys.length);
        Preconditions.checkArgument(leftKeys.length == filterNulls.length);

        if (null != nonEquiCondition && nonEquiCondition.isAlwaysTrue()) {
            this.nonEquiCondition = null;
        } else {
            this.nonEquiCondition = nonEquiCondition;
        }
    }

    @JsonIgnore
    public FlinkJoinType getJoinType() {
        return joinType;
    }

    @JsonIgnore
    public int[] getLeftKeys() {
        return leftKeys;
    }

    @JsonIgnore
    public int[] getRightKeys() {
        return rightKeys;
    }

    @JsonIgnore
    public boolean[] getFilterNulls() {
        return filterNulls;
    }

    @JsonIgnore
    public Optional<RexNode> getNonEquiCondition() {
        return Optional.ofNullable(nonEquiCondition);
    }

    /** Gets number of keys in join key. */
    @JsonIgnore
    public int getJoinKeySize() {
        return leftKeys.length;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MultiJoinSpec joinSpec = (MultiJoinSpec) o;
        return joinType == joinSpec.joinType
                && Arrays.equals(leftKeys, joinSpec.leftKeys)
                && Arrays.equals(rightKeys, joinSpec.rightKeys)
                && Arrays.equals(filterNulls, joinSpec.filterNulls)
                && Objects.equals(nonEquiCondition, joinSpec.nonEquiCondition);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(joinType, nonEquiCondition);
        result = 31 * result + Arrays.hashCode(leftKeys);
        result = 31 * result + Arrays.hashCode(rightKeys);
        result = 31 * result + Arrays.hashCode(filterNulls);
        return result;
    }
}
