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

package org.apache.kafka.streams.examples.pageview;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.HoppingWindows;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Properties;

// NOTE: this can only work with Java 8
public class PageViewUnTypedLambdaJob {

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(StreamsConfig.JOB_ID_CONFIG, "streams-pageview");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181");

        KStreamBuilder builder = new KStreamBuilder();

        //
        // register serializers/deserializers
        //
        builder.register(String.class, new StringSerializer(), new StringDeserializer());
        builder.register(Long.class, new LongSerializer(), new LongDeserializer());
        builder.register(JsonNode.class, new JsonSerializer(), new JsonDeserializer());

        //
        // define the topology
        //
        KStream<String, JsonNode> views = builder.stream(String.class, JsonNode.class, "streams-pageview-input");

        KStream<String, JsonNode> viewsByUser = views.map((dummy, record) -> new KeyValue<>(record.get("user").textValue(), record));//.returns(String.class, JsonNode.class);

        KTable<String, JsonNode> users = builder.table(String.class, JsonNode.class, "streams-userprofile-input");

        KTable<String, String> userRegions = users.mapValues(record -> record.get("region").textValue());

        /**
         * Exception in thread "main" org.apache.kafka.streams.kstream.InsufficientTypeInfoException: Invalid topology building: insufficient type information: key type of this stream
         at org.apache.kafka.streams.kstream.internals.AbstractStream.ensureJoinableWith(AbstractStream.java:65)
         at org.apache.kafka.streams.kstream.internals.KStreamImpl.leftJoin(KStreamImpl.java:414)
         at org.apache.kafka.streams.examples.pageview.PageViewUnTypedLambdaJob.main(PageViewUnTypedLambdaJob.java:74)
         */
        KStream<JsonNode, JsonNode> regionCount = viewsByUser
                .leftJoin(userRegions, (view, region) -> {
                    ObjectNode jNode = JsonNodeFactory.instance.objectNode();

                    return (JsonNode) jNode.put("user", view.get("user").textValue())
                                           .put("page", view.get("page").textValue())
                                           .put("region", region);
                })
                .map((user, viewRegion) -> new KeyValue<>(viewRegion.get("region").textValue(), viewRegion))
                .countByKey(HoppingWindows.of("GeoPageViewsWindow").with(7 * 24 * 60 * 60 * 1000))
                .toStream()
                .map((winView, count) -> {
                    ObjectNode keyNode = JsonNodeFactory.instance.objectNode();
                    keyNode.put("window-start", winView.window().start())
                           .put("region", winView.window().start());

                    ObjectNode valueNode = JsonNodeFactory.instance.objectNode();
                    keyNode.put("count", count);

                    return new KeyValue<>((JsonNode) keyNode, (JsonNode) valueNode);
                });

        // write to the result topic
        regionCount.to("streams-pageviewstats-output");

        //
        // run the job
        //
        KafkaStreams streams = new KafkaStreams(builder, props);
        streams.start();
    }
}
