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
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.utils.MockTime;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public class PubsubSenderTest {

    private static final int MAX_REQUEST_SIZE = 1024 * 1024;
    private static final short ACKS_ALL = -1;
    private static final int MAX_RETRIES = 0;
    private static final String CLIENT_ID = "clientId";
    private static final String METRIC_GROUP = "producer-metrics";
    private static final double EPS = 0.0001;
    private static final int MAX_BLOCK_TIMEOUT = 1000;
    private static final int REQUEST_TIMEOUT = 1000;

    private String topic = "test";
    private MockTime time = new MockTime();
    private int batchSize = 16 * 1024;
    private Metrics metrics = null;
    private PubsubAccumulator accumulator = null;
    private PubsubSender sender = null;
    private MockPubsubServer server;

    @Rule
    public Timeout globalTimeout = Timeout.seconds(15);

    @Before
    public void setup() {
        Map<String, String> metricTags = new LinkedHashMap<>();
        metricTags.put("client-id", CLIENT_ID);
        MetricConfig metricConfig = new MetricConfig().tags(metricTags);
        metrics = new Metrics(metricConfig, time);
        accumulator = new PubsubAccumulator(batchSize, 1024 * 1024, CompressionType.NONE, 0L, 0L, metrics, time);
        server = new MockPubsubServer();
        try {
            InProcessServerBuilder.forName("test").directExecutor().addService(server).build().start();
        } catch (IOException e) {

        }
        sender = new PubsubSender(InProcessChannelBuilder.forName("test").directExecutor().build(),
                this.accumulator,
                true,
                MAX_REQUEST_SIZE,
                MAX_RETRIES,
                metrics,
                time,
                REQUEST_TIMEOUT);
    }

    @After
    public void tearDown() {
        this.metrics.close();
    }

    @Test
    public void testSimple() throws Exception {
        long offset = 0;
        Future<RecordMetadata> future = accumulator.append(topic, 0L, "key".getBytes(), "value".getBytes(), null, MAX_BLOCK_TIMEOUT).future;
        sender.run(time.milliseconds()); // Sends produce request
        boolean found = false;
        for (int i = 0; i < 100; i++) {
            if (server.listen(1)) {
                found = true;
                break;
            }
            Thread.sleep(100);
        }
        if (!found) {
            fail("Server never received message");
        }
        server.respond(PublishResponse.newBuilder().addMessageIds("0").build());
        sender.run(time.milliseconds());
        assertEquals("All requests completed.", offset, (long) server.inFlight());
        assertNotNull("Request should be completed", future.get());
        assertEquals(offset, future.get().offset());
    }

//    /*
//     * Send multiple requests. Verify that the client side quota metrics have the right values
//     */
//    @Test
//    public void testQuotaMetrics() throws Exception {
//        final long offset = 0;
//        for (int i = 1; i <= 3; i++) {
//            Future<RecordMetadata> future = accumulator.append(tp, 0L, "key".getBytes(), "value".getBytes(), null, MAX_BLOCK_TIMEOUT).future;
//            sender.run(time.milliseconds()); // send produce request
//            client.respond(produceResponse(tp, offset, Errors.NONE.code(), 100 * i));
//            sender.run(time.milliseconds());
//        }
//        Map<MetricName, KafkaMetric> allMetrics = metrics.metrics();
//        KafkaMetric avgMetric = allMetrics.get(metrics.metricName("produce-throttle-time-avg", METRIC_GROUP, ""));
//        KafkaMetric maxMetric = allMetrics.get(metrics.metricName("produce-throttle-time-max", METRIC_GROUP, ""));
//        assertEquals(200, avgMetric.value(), EPS);
//        assertEquals(300, maxMetric.value(), EPS);
//    }
//
//    @Test
//    public void testRetries() throws Exception {
//        // create a sender with retries = 1
//        int maxRetries = 1;
//        Metrics m = new Metrics();
//        try {
//            PubsubSender sender = new PubsubSender(client,
//                    this.accumulator,
//                    false,
//                    MAX_REQUEST_SIZE,
//                    maxRetries,
//                    m,
//                    time,
//                    REQUEST_TIMEOUT);
//            // do a successful retry
//            Future<RecordMetadata> future = accumulator.append(tp, 0L, "key".getBytes(), "value".getBytes(), null, MAX_BLOCK_TIMEOUT).future;
//            sender.run(time.milliseconds()); // connect
//            sender.run(time.milliseconds()); // send produce request
//            String id = client.requests().peek().destination();
//            Node node = new Node(Integer.valueOf(id), "localhost", 0);
//            assertEquals(1, client.inFlightRequestCount());
//            assertTrue("Client ready status should be true", client.isReady(node, 0L));
//            client.disconnect(id);
//            assertEquals(0, client.inFlightRequestCount());
//            assertFalse("Client ready status should be false", client.isReady(node, 0L));
//            sender.run(time.milliseconds()); // receive error
//            sender.run(time.milliseconds()); // reconnect
//            sender.run(time.milliseconds()); // resend
//            assertEquals(1, client.inFlightRequestCount());
//            long offset = 0;
//            client.respond(produceResponse(tp, offset, Errors.NONE.code(), 0));
//            sender.run(time.milliseconds());
//            assertTrue("Request should have retried and completed", future.isDone());
//            assertEquals(offset, future.get().offset());
//
//            // do an unsuccessful retry
//            future = accumulator.append(tp, 0L, "key".getBytes(), "value".getBytes(), null, MAX_BLOCK_TIMEOUT).future;
//            sender.run(time.milliseconds()); // send produce request
//            for (int i = 0; i < maxRetries + 1; i++) {
//                client.disconnect(client.requests().peek().destination());
//                sender.run(time.milliseconds()); // receive error
//                sender.run(time.milliseconds()); // reconnect
//                sender.run(time.milliseconds()); // resend
//            }
//            sender.run(time.milliseconds());
//            completedWithError(future, Errors.NETWORK_EXCEPTION);
//        } finally {
//            m.close();
//        }
//    }
//
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
//
//    private void completedWithError(Future<RecordMetadata> future, Errors error) throws Exception {
//        assertTrue("Request should be completed", future.isDone());
//        try {
//            future.get();
//            fail("Should have thrown an exception.");
//        } catch (ExecutionException e) {
//            assertEquals(error.exception().getClass(), e.getCause().getClass());
//        }
//    }
//
//    private ProduceResponse produceResponse(TopicPartition tp, long offset, int error, int throttleTimeMs) {
//        ProduceResponse.PartitionResponse resp = new ProduceResponse.PartitionResponse((short) error, offset, Record.NO_TIMESTAMP);
//        Map<TopicPartition, ProduceResponse.PartitionResponse> partResp = Collections.singletonMap(tp, resp);
//        return new ProduceResponse(partResp, throttleTimeMs);
//    }

}
