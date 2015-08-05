/**
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

package org.apache.kafka.stream.internals;

import java.util.PriorityQueue;

public class PunctuationQueue {

    private PriorityQueue<PunctuationSchedule> pq = new PriorityQueue<>();

    public void schedule(PunctuationSchedule sched) {
        synchronized (pq) {
            pq.add(sched);
        }
    }

    public void close() {
        synchronized (pq) {
            pq.clear();
        }
    }

    public void mayPunctuate(long streamTime) {
        synchronized (pq) {
            PunctuationSchedule top = pq.peek();
            while (top != null && top.timestamp <= streamTime) {
                PunctuationSchedule sched = top;
                pq.poll();
                sched.processor().punctuate(streamTime);
                pq.add(sched.next());

                top = pq.peek();
            }
        }
    }

}
