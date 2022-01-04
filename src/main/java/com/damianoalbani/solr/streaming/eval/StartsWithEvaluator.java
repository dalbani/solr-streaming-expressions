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
package com.damianoalbani.solr.streaming.eval;

import org.apache.solr.client.solrj.io.eval.ManyValueWorker;
import org.apache.solr.client.solrj.io.eval.RecursiveBooleanEvaluator;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;

public class StartsWithEvaluator extends RecursiveBooleanEvaluator implements ManyValueWorker {

    protected static final long serialVersionUID = 1L;

    public StartsWithEvaluator(StreamExpression expression, StreamFactory factory) throws IOException {
        super(expression, factory);

        if (containedEvaluators.size() < 2) {
            throw new IOException(String.format(Locale.ROOT,
                    "Invalid expression %s - expecting at least two values but found %d", expression, containedEvaluators.size()));
        }
    }

    @Override
    protected Checker constructChecker(Object value) throws IOException {
        if (value instanceof String) {
            return (StringChecker) (left, right) -> ((String) left).startsWith((String) right);
        }

        throw new IOException(String.format(Locale.ROOT, "Unable to check %s(...) for values of type '%s'",
                constructingFactory.getFunctionName(getClass()), Objects.requireNonNull(value.getClass().getSimpleName(), "<null>")));
    }

}
