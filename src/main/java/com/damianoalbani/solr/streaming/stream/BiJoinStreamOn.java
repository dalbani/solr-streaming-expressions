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

import org.apache.solr.client.solrj.io.comp.FieldComparator;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.eval.RecursiveBooleanEvaluator;
import org.apache.solr.client.solrj.io.stream.PushBackStream;
import org.apache.solr.client.solrj.io.stream.TupleStream;
import org.apache.solr.client.solrj.io.stream.expr.Expressible;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

import java.io.IOException;

// Largely based on "org.apache.solr.client.solrj.io.stream.BiJoinStream".

public abstract class BiJoinStreamOn extends JoinStreamOn implements Expressible {

    protected PushBackStream leftStream;
    protected PushBackStream rightStream;

    // This is used to determine whether we should iterate the left or right side (depending on stream order).
    // It is built from the incoming equalitor and streams' comparators.
    protected StreamComparator iterationComparator;
    protected StreamComparator leftStreamComparator;
    protected StreamComparator rightStreamComparator;

    public BiJoinStreamOn(TupleStream leftStream, TupleStream rightStream, RecursiveBooleanEvaluator evaluator) throws IOException {
        super(evaluator, leftStream, rightStream);
        init();
    }

    public BiJoinStreamOn(StreamExpression expression, StreamFactory factory) throws IOException {
        super(expression, factory);
        init();
    }

    private void init() throws IOException {
        leftStream = getStream(0);
        rightStream = getStream(1);

        // iterationComparator is a combination of the equalitor and the comp from each stream. This can easily be done by
        // grabbing the first N parts of each comp where N is the number of parts in the equalitor. Because we've already
        // validated tuple order (the comps) then we know this can be done.
        iterationComparator = createSideComparator(leftStream.getStreamSort());
        leftStreamComparator = createSideComparator(leftStream.getStreamSort());
        rightStreamComparator = createSideComparator(rightStream.getStreamSort());
    }

    private StreamComparator createSideComparator(StreamComparator comp) throws IOException {
        if (comp instanceof FieldComparator) {
            FieldComparator fieldComparator = (FieldComparator) comp;
            return new FieldComparator(fieldComparator.getLeftFieldName(), fieldComparator.getRightFieldName(),
                    fieldComparator.getOrder());
        } else {
            throw new IOException("Failed to create an side comparator");
        }
    }

}
