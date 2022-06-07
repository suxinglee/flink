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

package org.apache.flink.connector.pulsar.source.reader.split;

import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.connector.base.source.reader.RecordsBySplits;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.connector.pulsar.source.config.SourceConfiguration;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.StopCursor;
import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicPartition;
import org.apache.flink.connector.pulsar.source.reader.deserializer.PulsarDeserializationSchema;
import org.apache.flink.connector.pulsar.source.reader.message.PulsarMessage;
import org.apache.flink.connector.pulsar.source.reader.message.PulsarMessageCollector;
import org.apache.flink.connector.pulsar.source.split.PulsarPartitionSplit;
import org.apache.flink.util.Preconditions;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.KeySharedPolicy;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.flink.connector.pulsar.common.utils.PulsarExceptionUtils.sneakyClient;
import static org.apache.flink.connector.pulsar.source.config.PulsarSourceConfigUtils.createConsumerBuilder;

/**
 * The common partition split reader.
 *
 * @param <OUT> the type of the pulsar source message that would be serialized to downstream.
 */
abstract class PulsarPartitionSplitReaderBase<OUT>
        implements SplitReader<PulsarMessage<OUT>, PulsarPartitionSplit> {
    private static final Logger LOG = LoggerFactory.getLogger(PulsarPartitionSplitReaderBase.class);

    protected final PulsarClient pulsarClient;
    protected final PulsarAdmin pulsarAdmin;
    protected final SourceConfiguration sourceConfiguration;
    protected final PulsarDeserializationSchema<OUT> deserializationSchema;
    protected final AtomicBoolean wakeup;

    protected Consumer<?> pulsarConsumer;
    protected PulsarPartitionSplit registeredSplit;

    protected PulsarPartitionSplitReaderBase(
            PulsarClient pulsarClient,
            PulsarAdmin pulsarAdmin,
            SourceConfiguration sourceConfiguration,
            PulsarDeserializationSchema<OUT> deserializationSchema) {
        this.pulsarClient = pulsarClient;
        this.pulsarAdmin = pulsarAdmin;
        this.sourceConfiguration = sourceConfiguration;
        this.deserializationSchema = deserializationSchema;
        this.wakeup = new AtomicBoolean(false);
    }

    @Override
    public RecordsWithSplitIds<PulsarMessage<OUT>> fetch() throws IOException {
        RecordsBySplits.Builder<PulsarMessage<OUT>> builder = new RecordsBySplits.Builder<>();

        // Return when no split registered to this reader.
        if (pulsarConsumer == null || registeredSplit == null) {
            return builder.build();
        }

        // Set wakeup to false for start consuming.
        wakeup.compareAndSet(true, false);

        StopCursor stopCursor = registeredSplit.getStopCursor();
        String splitId = registeredSplit.splitId();
        PulsarMessageCollector<OUT> collector = new PulsarMessageCollector<>(splitId, builder);
        Deadline deadline = Deadline.fromNow(sourceConfiguration.getMaxFetchTime());

        // Consume message from pulsar until it was woken up by flink reader.
        for (int messageNum = 0;
                messageNum < sourceConfiguration.getMaxFetchRecords()
                        && deadline.hasTimeLeft()
                        && isNotWakeup();
                messageNum++) {
            try {
                Duration timeout = deadline.timeLeftIfAny();
                Message<?> message = pollMessage(timeout);
                if (message == null) {
                    break;
                }

                collector.setMessage(message);

                // Deserialize message by DeserializationSchema or Pulsar Schema.
                deserializationSchema.deserialize(message, collector);

                // Acknowledge message if needed.
                finishedPollMessage(message);

                if (stopCursor.shouldStop(message)) {
                    builder.addFinishedSplit(splitId);
                    break;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (TimeoutException e) {
                break;
            } catch (ExecutionException e) {
                LOG.error("Error in polling message from pulsar consumer.", e);
                break;
            } catch (Exception e) {
                throw new IOException(e);
            }
        }

        return builder.build();
    }

    @Override
    public void handleSplitsChanges(SplitsChange<PulsarPartitionSplit> splitsChanges) {
        LOG.debug("Handle split changes {}", splitsChanges);

        // Get all the partition assignments and stopping offsets.
        if (!(splitsChanges instanceof SplitsAddition)) {
            throw new UnsupportedOperationException(
                    String.format(
                            "The SplitChange type of %s is not supported.",
                            splitsChanges.getClass()));
        }

        if (registeredSplit != null) {
            throw new IllegalStateException("This split reader have assigned split.");
        }

        List<PulsarPartitionSplit> newSplits = splitsChanges.splits();
        Preconditions.checkArgument(
                newSplits.size() == 1, "This pulsar split reader only support one split.");
        PulsarPartitionSplit newSplit = newSplits.get(0);

        // Create pulsar consumer.
        Consumer<?> consumer = createPulsarConsumer(newSplit);

        // Open start & stop cursor.
        newSplit.open(pulsarAdmin);

        // Start Consumer.
        startConsumer(newSplit, consumer);

        LOG.info("Register split {} consumer for current reader.", newSplit);
        this.registeredSplit = newSplit;
        this.pulsarConsumer = consumer;
    }

    @Override
    public void wakeUp() {
        wakeup.compareAndSet(false, true);
    }

    @Override
    public void close() {
        if (pulsarConsumer != null) {
            sneakyClient(() -> pulsarConsumer.close());
        }
    }

    @Nullable
    protected abstract Message<?> pollMessage(Duration timeout)
            throws ExecutionException, InterruptedException, PulsarClientException;

    protected abstract void finishedPollMessage(Message<?> message);

    protected abstract void startConsumer(PulsarPartitionSplit split, Consumer<?> consumer);

    // --------------------------- Helper Methods -----------------------------

    protected boolean isNotWakeup() {
        return !wakeup.get();
    }

    /**
     * Create a specified {@link Consumer} by the given split information. If using pulsar schema,
     * then use the pulsar schema, if using flink schema, then use a Schema.BYTES
     */
    protected Consumer<?> createPulsarConsumer(PulsarPartitionSplit split) {
        return createPulsarConsumer(split.getPartition());
    }

    protected Consumer<?> createPulsarConsumer(TopicPartition partition) {
        Schema<?> schema = deserializationSchema.schema();

        ConsumerBuilder<?> consumerBuilder =
                createConsumerBuilder(pulsarClient, schema, sourceConfiguration);

        consumerBuilder.topic(partition.getFullTopicName());

        // Add KeySharedPolicy for Key_Shared subscription.
        if (sourceConfiguration.getSubscriptionType() == SubscriptionType.Key_Shared) {
            KeySharedPolicy policy =
                    KeySharedPolicy.stickyHashRange().ranges(partition.getPulsarRange());
            consumerBuilder.keySharedPolicy(policy);
        }

        // Create the consumer configuration by using common utils.
        return sneakyClient(consumerBuilder::subscribe);
    }
}
