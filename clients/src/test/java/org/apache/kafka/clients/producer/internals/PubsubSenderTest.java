/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.clients.producer.internals;

import com.google.pubsub.v1.PublishResponse;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.utils.MockTime;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class PubsubSenderTest {

    private static final int MAX_REQUEST_SIZE = 1024 * 1024;
    private static final short ACKS_ALL = -1;
    private static final int MAX_RETRIES = 0;
    private static final String CLIENT_ID = "clientId";
    private static final String METRIC_GROUP = "producer-metrics";
    private static final double EPS = 0.0001;
    private static final int MAX_BLOCK_TIMEOUT = 1000;
    private static final int REQUEST_TIMEOUT = 10000;

    private String topic = "test-topic";
    private MockTime time = new MockTime();
    private int batchSize = 16 * 1024;
    private Metrics metrics = null;
    private PubsubAccumulator accumulator = null;

//    @Rule
//    public Timeout globalTimeout = Timeout.seconds(15);

    @Before
    public void setup() {
        Map<String, String> metricTags = new LinkedHashMap<>();
        metricTags.put("client-id", CLIENT_ID);
        MetricConfig metricConfig = new MetricConfig().tags(metricTags);
        metrics = new Metrics(metricConfig, time);
        accumulator = new PubsubAccumulator(batchSize, 1024 * 1024, CompressionType.NONE, 0L, 0L, metrics, time);
    }

    @After
    public void tearDown() {
        this.metrics.close();
    }

    @Test
    public void testSimple() throws Exception {
        MockPubsubServer server = newServer("testSimple");
        PubsubSender sender = newSender("testSimple", MAX_RETRIES);
        long offset = 0;
        Future<RecordMetadata> future = accumulator.append(topic, 0L, "key".getBytes(), "value".getBytes(), null, MAX_BLOCK_TIMEOUT).future;
        sender.run(time.milliseconds()); // Sends produce request
        boolean found = false;
        for (int i = 0; i < 100; i++) {
            if (server.listen(1)) {
                found = true;
                break;
            }
            Thread.sleep(50);
        }
        if (!found) {
            fail("Server never received message");
        }
        server.respond(PublishResponse.newBuilder().addMessageIds("0").build());
        assertEquals("All requests completed.", offset, (long) server.inFlightCount());
        assertNotNull("Request should be completed", future.get());
        assertEquals(offset, future.get().offset());
    }

    @Test
    public void testRetries() throws Exception {
        MockPubsubServer server = newServer("testRetries");
        PubsubSender sender = newSender("testRetries", 1);
        // do a successful retry
        Future<RecordMetadata> future = accumulator.append(topic, 0L, "key".getBytes(), "value".getBytes(), null, MAX_BLOCK_TIMEOUT).future;
        sender.run(time.milliseconds()); // send produce request
        boolean found = false;
        for (int i = 0; i < 100; i++) {
            if (server.listen(1)) {
                found = true;
                break;
            }
            Thread.sleep(50);
        }
        if (!found) {
            fail("Server never received message");
        }
        server.disconnect();
        assertEquals("All requests completed.", 0, server.inFlightCount());
        completedWithError(future, Errors.NETWORK_EXCEPTION);
        sender.run(time.milliseconds()); // send second produce request
        found = false;
        for (int i = 0; i < 100; i++) {
            if (server.listen(1)) {
                found = true;
                break;
            }
            Thread.sleep(50);
        }
        if (!found) {
            fail("Server never received message");
        }
        long offset = 0;
        server.respond(PublishResponse.newBuilder().addMessageIds(Long.toString(offset)).build());
        Thread.sleep(50);
        assertTrue("Request should have retried and completed", future.isDone());
        assertEquals(offset, future.get().offset());

//        // do an unsuccessful retry
//        future = accumulator.append(topic, 0L, "key".getBytes(), "value".getBytes(), null, MAX_BLOCK_TIMEOUT).future;
//        sender.run(time.milliseconds()); // send produce request
//        for (int i = 0; i < maxRetries + 1; i++) {
//            server.disconnect();
//            Thread.sleep(50);
//        }
    }

//    @Test
//    public void testSendInOrder() throws Exception {
//        int maxRetries = 1;
//        Metrics m = new Metrics();
//        try {
//            Sender sender = new Sender(client,
//                    metadata,
//                    this.accumulator,
//                    true,
//                    MAX_REQUEST_SIZE,
//                    ACKS_ALL,
//                    maxRetries,
//                    m,
//                    time,
//                    REQUEST_TIMEOUT);
//
//            // Create a two broker cluster, with partition 0 on broker 0 and partition 1 on broker 1
//            Cluster cluster1 = TestUtils.clusterWith(2, "test", 2);
//            metadata.update(cluster1, time.milliseconds());
//
//            // Send the first message.
//            TopicPartition tp2 = new TopicPartition("test", 1);
//            accumulator.append(tp2, 0L, "key1".getBytes(), "value1".getBytes(), null, MAX_BLOCK_TIMEOUT);
//            sender.run(time.milliseconds()); // connect
//            sender.run(time.milliseconds()); // send produce request
//            String id = client.requests().peek().destination();
//            assertEquals(ApiKeys.PRODUCE.id, client.requests().peek().header().apiKey());
//            Node node = new Node(Integer.valueOf(id), "localhost", 0);
//            assertEquals(1, client.inFlightRequestCount());
//            assertTrue("Client ready status should be true", client.isReady(node, 0L));
//
//            time.sleep(900);
//            // Now send another message to tp2
//            accumulator.append(tp2, 0L, "key2".getBytes(), "value2".getBytes(), null, MAX_BLOCK_TIMEOUT);
//
//            // Update metadata before sender receives response from broker 0. Now partition 2 moves to broker 0
//            Cluster cluster2 = TestUtils.singletonCluster("test", 2);
//            metadata.update(cluster2, time.milliseconds());
//            // Sender should not send the second message to node 0.
//            sender.run(time.milliseconds());
//            assertEquals(1, client.inFlightRequestCount());
//        } finally {
//            m.close();
//        }
//    }
//
//    /**
//     * Tests that topics are added to the metadata list when messages are available to send
//     * and expired if not used during a metadata refresh interval.
//     */
//    @Test
//    public void testMetadataTopicExpiry() throws Exception {
//        long offset = 0;
//        metadata.update(Cluster.empty(), time.milliseconds());
//
//        Future<RecordMetadata> future = accumulator.append(tp, time.milliseconds(), "key".getBytes(), "value".getBytes(), null, MAX_BLOCK_TIMEOUT).future;
//        sender.run(time.milliseconds());
//        assertTrue("Topic not added to metadata", metadata.containsTopic(tp.topic()));
//        metadata.update(cluster, time.milliseconds());
//        sender.run(time.milliseconds());  // send produce request
//        client.respond(produceResponse(tp, offset++, Errors.NONE.code(), 0));
//        sender.run(time.milliseconds());
//        assertEquals("Request completed.", 0, client.inFlightRequestCount());
//        sender.run(time.milliseconds());
//        assertTrue("Request should be completed", future.isDone());
//
//        assertTrue("Topic not retained in metadata list", metadata.containsTopic(tp.topic()));
//        time.sleep(Metadata.TOPIC_EXPIRY_MS);
//        metadata.update(Cluster.empty(), time.milliseconds());
//        assertFalse("Unused topic has not been expired", metadata.containsTopic(tp.topic()));
//        future = accumulator.append(tp, time.milliseconds(), "key".getBytes(), "value".getBytes(), null, MAX_BLOCK_TIMEOUT).future;
//        sender.run(time.milliseconds());
//        assertTrue("Topic not added to metadata", metadata.containsTopic(tp.topic()));
//        metadata.update(cluster, time.milliseconds());
//        sender.run(time.milliseconds());  // send produce request
//        client.respond(produceResponse(tp, offset++, Errors.NONE.code(), 0));
//        sender.run(time.milliseconds());
//        assertEquals("Request completed.", 0, client.inFlightRequestCount());
//        sender.run(time.milliseconds());
//        assertTrue("Request should be completed", future.isDone());
//    }

    private void completedWithError(Future<RecordMetadata> future, Errors error) throws Exception {
        try {
            future.get();
            fail("Should have thrown an exception.");
        } catch (ExecutionException e) {
            assertEquals(error.exception().getClass(), e.getCause().getClass());
        }
    }

    private PubsubSender newSender(String channelName, int retries) {
        return new PubsubSender(InProcessChannelBuilder.forName(channelName).directExecutor().build(),
                this.accumulator,
                true,
                MAX_REQUEST_SIZE,
                retries,
                metrics,
                time,
                REQUEST_TIMEOUT);
    }

    private MockPubsubServer newServer(String channelName) {
        MockPubsubServer out = new MockPubsubServer();
        try {
            InProcessServerBuilder.forName(channelName).directExecutor().addService(out).build().start();
        } catch (IOException e) {
            return null;
        }
        return out;
    }

//    private ProduceResponse produceResponse(TopicPartition tp, long offset, int error, int throttleTimeMs) {
//        ProduceResponse.PartitionResponse resp = new ProduceResponse.PartitionResponse((short) error, offset, Record.NO_TIMESTAMP);
//        Map<TopicPartition, ProduceResponse.PartitionResponse> partResp = Collections.singletonMap(tp, resp);
//        return new ProduceResponse(partResp, throttleTimeMs);
//    }

}
