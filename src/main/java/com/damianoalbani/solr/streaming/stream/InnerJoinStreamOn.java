/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.damianoalbani.solr.streaming.stream;

import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.eval.RecursiveBooleanEvaluator;
import org.apache.solr.client.solrj.io.stream.TupleStream;
import org.apache.solr.client.solrj.io.stream.expr.Expressible;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

import java.io.IOException;
import java.util.LinkedList;

// Largely based on "org.apache.solr.client.solrj.io.stream.InnerJoinStream".

public class InnerJoinStreamOn extends BiJoinStreamOn implements Expressible {

    public static final String LEFT_SIDE_TUPLE_FIELD = "_left_";
    public static final String RIGHT_SIDE_TUPLE_FIELD = "_right_";

    private final LinkedList<Tuple> joinedTuples = new LinkedList<>();
    private final LinkedList<Tuple> leftTupleGroup = new LinkedList<>();
    private final LinkedList<Tuple> rightTupleGroup = new LinkedList<>();

    public InnerJoinStreamOn(TupleStream leftStream, TupleStream rightStream, RecursiveBooleanEvaluator evaluator) throws IOException {
        super(leftStream, rightStream, evaluator);
    }

    public InnerJoinStreamOn(StreamExpression expression, StreamFactory factory) throws IOException {
        super(expression, factory);
    }

    public Tuple read() throws IOException {
        // if we've already figured out the next joined tuple then just return it
        if (!joinedTuples.isEmpty()) {
            return joinedTuples.removeFirst();
        }

        // keep going until we find something to return or (left or right) are empty
        while (true) {
            if (leftTupleGroup.isEmpty()) {
                Tuple firstMember = loadEqualTupleGroup(leftStream, leftTupleGroup, leftStreamComparator);

                // if first member of group is EOF then we're done
                if (firstMember.EOF) {
                    return firstMember;
                }
            }

            if (rightTupleGroup.isEmpty()) {
                Tuple firstMember = loadEqualTupleGroup(rightStream, rightTupleGroup, rightStreamComparator);

                // if first member of group is EOF then we're done
                if (firstMember.EOF) {
                    return firstMember;
                }
            }

            // At this point we know both left and right groups have at least 1 member
            boolean result = (boolean) evaluator.evaluate(new Tuple(LEFT_SIDE_TUPLE_FIELD, leftTupleGroup.get(0),
                    RIGHT_SIDE_TUPLE_FIELD, rightTupleGroup.get(0)));
            if (result) {
                // The groups are equal. Join em together and build the joinedTuples
                for (Tuple left : leftTupleGroup) {
                    for (Tuple right : rightTupleGroup) {
                        Tuple clone = left.clone();
                        clone.merge(right);
                        joinedTuples.add(clone);
                    }
                }

                // Cause each to advance next time we need to look
                leftTupleGroup.clear();
                rightTupleGroup.clear();

                return joinedTuples.removeFirst();
            } else {
                int c = iterationComparator.compare(leftTupleGroup.get(0), rightTupleGroup.get(0));
                if (c < 0) {
                    // advance left
                    leftTupleGroup.clear();
                } else {
                    // advance right
                    rightTupleGroup.clear();
                }
            }
        }
    }

    @Override
    public StreamComparator getStreamSort() {
        return iterationComparator;
    }

}
