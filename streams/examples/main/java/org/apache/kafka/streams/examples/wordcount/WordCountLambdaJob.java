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

package org.apache.kafka.streams.examples.wordcount;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.UnlimitedWindows;

import java.util.Arrays;
import java.util.Properties;

// NOTE: this can only work with Java 8
public class WordCountLambdaJob {

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(StreamsConfig.JOB_ID_CONFIG, "streams-wordcount");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181");

        // can specify underlying client configs if necessary
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KStreamBuilder builder = new KStreamBuilder();

        builder.register(String.class, new StringSerializer(), new StringDeserializer());
        builder.register(Long.class, new LongSerializer(), new LongDeserializer());
        builder.register(JsonNode.class, new JsonSerializer(), new JsonDeserializer());

        KStream<String, String> source = builder.stream(String.class, String.class, "streams-file-input");

        /**
         * Exception in thread "main" org.apache.kafka.streams.kstream.InsufficientTypeInfoException: Invalid topology building: insufficient type information: key type
         at org.apache.kafka.streams.kstream.internals.AbstractStream.getWindowedKeyType(AbstractStream.java:130)
         at org.apache.kafka.streams.kstream.internals.KStreamImpl.aggregateByKey(KStreamImpl.java:475)
         at org.apache.kafka.streams.kstream.internals.KStreamImpl.countByKey(KStreamImpl.java:521)
         at org.apache.kafka.streams.examples.wordcount.WordCountLambdaJob.main(WordCountLambdaJob.java:67)
         */
        KStream<String, JsonNode> counts = source
                .flatMapValues(value -> Arrays.asList(value.toLowerCase().split(" ")))
                .map((key, value) -> new KeyValue<>(value, value))//.returns(String.class, String.class)
                .countByKey(UnlimitedWindows.of("Counts").startOn(0L))
                .toStream()
                .map((winKey, value) -> {
                    ObjectNode jNode = JsonNodeFactory.instance.objectNode();

                    jNode.put("word", winKey.value())
                         .put("count", value);

                    return new KeyValue<>((String) null, (JsonNode) jNode);
                });//.returns(String.class, JsonNode.class);

        counts.to("streams-wordcount-output");

        KafkaStreams streams = new KafkaStreams(builder, props);
        streams.start();
    }
}
