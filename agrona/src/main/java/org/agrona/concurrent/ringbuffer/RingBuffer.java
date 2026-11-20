/*
 * Copyright 2014-2025 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.agrona.concurrent.ringbuffer;

import org.agrona.DirectBuffer;
import org.agrona.concurrent.*;

import java.util.concurrent.TimeUnit;

/**
 * Ring-buffer for the concurrent exchanging of binary encoded messages from producer(s) to consumer(s)
 * in a FIFO manner.
 * <p>
 * Provides two non-blocking write methods: {@link #write(int, DirectBuffer, int, int)} for copying a message
 * to the ring-buffer, and {@link #tryClaim(int, int)} for zero-copy writes where the caller writes directly
 * to the ring-buffer memory. Messages are read by consumers using one of the {@code read} or
 * {@code controlledRead} methods.
 */
public interface RingBuffer
{
    /**
     * Record type is padding to prevent fragmentation in the buffer.
     */
    int PADDING_MSG_TYPE_ID = -1;

    /**
     * Buffer has insufficient capacity to record a message or satisfy {@link #tryClaim(int, int)} request.
     */
    int INSUFFICIENT_CAPACITY = -2;

    /**
     * Get the capacity of the ring-buffer in bytes for exchange.
     *
     * @return the capacity of the ring-buffer in bytes for exchange.
     */
    int capacity();

    /**
     * Non-blocking write of a message to an underlying ring-buffer.
     *
     * <p>
     * Example usage:
     * <pre>{@code
     * // Create a ring buffer with a direct buffer of 1024 bytes
     * ByteBuffer byteBuffer = ByteBuffer.allocateDirect(1024);
     * RingBuffer ringBuffer = new OneToOneRingBuffer(new UnsafeBuffer(byteBuffer));
     *
     * // Pre-allocate a reusable buffer (cached for the lifetime of the writer)
     * MutableDirectBuffer srcBuffer =
     *     new UnsafeBuffer(ByteBuffer.allocateDirect(Long.BYTES));
     *
     * // Write a message using the cached buffer
     * srcBuffer.putLong(0, 12345L);
     * if (!ringBuffer.write(1, srcBuffer, 0, Long.BYTES))
     * {
     *     // Handle insufficient capacity (e.g., retry, log, or drop message)
     * }
     * }</pre>
     *
     * @param msgTypeId type of the message encoding.
     * @param srcBuffer containing the encoded binary message.
     * @param offset    at which the encoded message begins.
     * @param length    of the encoded message in bytes.
     * @return true if written to the ring-buffer, or false if insufficient space exists.
     * @throws IllegalArgumentException if the {@code length} is negative or is greater than {@link #maxMsgLength()}.
     */
    boolean write(int msgTypeId, DirectBuffer srcBuffer, int offset, int length);

    /**
     * Write a message, applying bounded back-pressure while the ring-buffer does not have enough free capacity.
     * <p>
     * Instead of overwriting unread records (which would expose the consumer to torn messages) or spinning forever
     * when the consumer is stalled (e.g. during a restart), this method polls the consumer position published in the
     * ring-buffer trailer and idles for at most {@code timeout} units of time before giving up. The calling thread is
     * therefore guaranteed never to block indefinitely: a timeout or an interrupt results in a graceful failure
     * return rather than a hung producer.
     *
     * @param msgTypeId type of the message encoding.
     * @param srcBuffer containing the encoded binary message.
     * @param offset    at which the encoded message begins.
     * @param length    of the encoded message in bytes.
     * @param idleStrategy strategy used to back off (spin/yield/park) between retries while the buffer is full.
     * @param timeout   maximum time to wait for capacity, must be non-negative.
     * @param unit      time unit of the timeout.
     * @return true if the message was written, false if the timeout elapsed or the thread was interrupted first.
     * @throws IllegalArgumentException if the {@code length} is negative or is greater than {@link #maxMsgLength()}.
     * @see #write(int, DirectBuffer, int, int)
     */
    default boolean write(
        final int msgTypeId,
        final DirectBuffer srcBuffer,
        final int offset,
        final int length,
        final IdleStrategy idleStrategy,
        final long timeout,
        final TimeUnit unit)
    {
        if (write(msgTypeId, srcBuffer, offset, length))
        {
            return true;
        }

        final long deadlineNs = System.nanoTime() + unit.toNanos(timeout);
        idleStrategy.reset();

        while (System.nanoTime() - deadlineNs < 0)
        {
            if (Thread.currentThread().isInterrupted())
            {
                return false;
            }

            idleStrategy.idle();

            if (write(msgTypeId, srcBuffer, offset, length))
            {
                return true;
            }
        }

        return false;
    }

    /**
     * Try to claim a space applying bounded back-pressure while the ring-buffer does not have enough free capacity.
     * <p>
     * Behaves like {@link #tryClaim(int, int)} but, instead of returning {@link #INSUFFICIENT_CAPACITY} immediately,
     * polls the consumer position and idles for at most {@code timeout} units of time for space to become available.
     * The wait is bounded so the producer thread can never hang on a stalled or restarting consumer.
     *
     * @param msgTypeId type of the message encoding. Will be written into the header upon successful claim.
     * @param length    of the claim in bytes. A claim length cannot be greater than {@link #maxMsgLength()}.
     * @param idleStrategy strategy used to back off (spin/yield/park) between retries while the buffer is full.
     * @param timeout   maximum time to wait for capacity, must be non-negative.
     * @param unit      time unit of the timeout.
     * @return a non-zero index into the underlying ring-buffer at which encoded message begins,
     * {@link #INSUFFICIENT_CAPACITY} if the timeout elapsed or the thread was interrupted before space became
     * available.
     * @throws IllegalArgumentException if the {@code msgTypeId} is less than {@code 1}.
     * @throws IllegalArgumentException if the {@code length} is negative or is greater than {@link #maxMsgLength()}.
     * @see #tryClaim(int, int)
     * @see #commit(int)
     * @see #abort(int)
     */
    default int tryClaim(
        final int msgTypeId,
        final int length,
        final IdleStrategy idleStrategy,
        final long timeout,
        final TimeUnit unit)
    {
        int index = tryClaim(msgTypeId, length);
        if (INSUFFICIENT_CAPACITY != index)
        {
            return index;
        }

        final long deadlineNs = System.nanoTime() + unit.toNanos(timeout);
        idleStrategy.reset();

        while (System.nanoTime() - deadlineNs < 0)
        {
            if (Thread.currentThread().isInterrupted())
            {
                return INSUFFICIENT_CAPACITY;
            }

            idleStrategy.idle();

            index = tryClaim(msgTypeId, length);
            if (INSUFFICIENT_CAPACITY != index)
            {
                return index;
            }
        }

        return INSUFFICIENT_CAPACITY;
    }

    /**
     * Try to claim a space in the underlying ring-buffer into which a message can be written with zero copy semantics.
     * Once the message has been written then {@link #commit(int)} should be called thus making it available to be
     * consumed. Alternatively a claim can be aborted using {@link #abort(int)} method.
     * <p>
     * Claiming a space in the ring-buffer means that the consumer will not be able to consume past the claim until
     * the claimed space is either committed or aborted. Producers will be able to write message even when outstanding
     * claims exist.
     * <p>
     * An example of using {@code tryClaim}:
     * <pre>
     * {@code
     *     final RingBuffer ringBuffer = ...;
     *
     *     final int index = ringBuffer.tryClaim(msgTypeId, messageLength);
     *     if (index > 0)
     *     {
     *         try
     *         {
     *             final AtomicBuffer buffer = ringBuffer.buffer();
     *             // Work with the buffer directly using the index
     *             ...
     *         }
     *         finally
     *         {
     *             ringBuffer.commit(index); // commit message
     *         }
     *     }
     * }
     * </pre>
     * <p>
     * Ensure that claimed space is released even in case of an exception:
     * <pre>
     * {@code
     *     final RingBuffer ringBuffer = ...;
     *
     *     final int index = ringBuffer.tryClaim(msgTypeId, messageLength);
     *     if (index > 0)
     *     {
     *         try
     *         {
     *             final AtomicBuffer buffer = ringBuffer.buffer();
     *             // Work with the buffer directly using the index
     *             ...
     *             ringBuffer.commit(index); // commit message
     *         }
     *         catch (final Exception ex)
     *         {
     *             ringBuffer.abort(index); // allow consumer to proceed
     *             ...
     *         }
     *     }
     * }
     * </pre>
     *
     * @param msgTypeId type of the message encoding. Will be written into the header upon successful claim.
     * @param length    of the claim in bytes. A claim length cannot be greater than {@link #maxMsgLength()}.
     * @return a non-zero index into the underlying ring-buffer at which encoded message begins, otherwise returns
     * {@link #INSUFFICIENT_CAPACITY} indicating that there is not enough free space in the buffer.
     * @throws IllegalArgumentException if the {@code msgTypeId} is less than {@code 1}.
     * @throws IllegalArgumentException if the {@code length} is negative or is greater than {@link #maxMsgLength()}.
     * @see #commit(int)
     * @see #abort(int)
     */
    int tryClaim(int msgTypeId, int length);

    /**
     * Commit message that was written in the previously claimed space thus making it available to the consumer.
     *
     * @param index at which the encoded message begins, i.e. value returned from the {@link #tryClaim(int, int)} call.
     * @throws IllegalArgumentException if the {@code index} is out of bounds.
     * @throws IllegalStateException    if this method is called after {@link #abort(int)} or was already invoked for
     *                                  the given {@code index}.
     * @see #tryClaim(int, int)
     */
    void commit(int index);

    /**
     * Abort claim and allow consumer to proceed after the claimed length. Aborting turns unused space into padding,
     * i.e. changes type of the message to {@link #PADDING_MSG_TYPE_ID}.
     *
     * @param index at which the encoded message begins, i.e. value returned from the {@link #tryClaim(int, int)} call.
     * @throws IllegalArgumentException if the {@code index} is out of bounds.
     * @throws IllegalStateException    if this method is called after {@link #commit(int)} or was already invoked for
     *                                  the given {@code index}.
     * @see #tryClaim(int, int)
     */
    void abort(int index);

    /**
     * Read as many messages as are available to the end of the ring buffer.
     * <p>
     * If the ring buffer wraps or encounters a type of record, such a padding record, then an implementation
     * may choose to return and expect the caller to try again. The {@link #size()} method may be called to
     * determine of a backlog of message bytes remains in the ring buffer.
     *
     * @param handler to be called for processing each message in turn.
     * @return the number of messages that have been processed.
     */
    int read(MessageHandler handler);

    /**
     * Read as many messages as are available to end of the ring buffer to up a supplied maximum.
     *
     * <p>
     * Example usage:
     * <pre>{@code
     * ringBuffer.read(
     *     (msgTypeId, buffer, index, length) ->
     *     {
     *         final long value = buffer.getLong(index);
     *         // Process value
     *     }
     * );
     * }</pre>
     *
     * <p>
     * If the ring buffer wraps or encounters a type of record, such a padding record, then an implementation
     * may choose to return and expect the caller to try again. The {@link #size()} method may be called to
     * determine of a backlog of message bytes remains in the ring buffer.
     *
     * @param handler           to be called for processing each message in turn.
     * @param messageCountLimit the number of messages will be read in a single invocation.
     * @return the number of messages that have been processed.
     */
    int read(MessageHandler handler, int messageCountLimit);

    /**
     * Read as many messages as are available to the end of the ring buffer with the handler able to control progress.
     * <p>
     * If the ring buffer wraps or encounters a type of record, such a padding record, then an implementation
     * may choose to return and expect the caller to try again. The {@link #size()} method may be called to
     * determine of a backlog of message bytes remains in the ring buffer.
     *
     * @param handler to be called for processing each message in turn which will return how to progress.
     * @return the number of messages that have been processed.
     */
    int controlledRead(ControlledMessageHandler handler);

    /**
     * Read messages up to a limit of available to the end of the ring buffer with the handler able to control progress.
     * <p>
     * If the ring buffer wraps or encounters a type of record, such a padding record, then an implementation
     * may choose to return and expect the caller to try again. The {@link #size()} method may be called to
     * determine of a backlog of message bytes remains in the ring buffer.
     *
     * @param handler           to be called for processing each message in turn which will return how to progress.
     * @param messageCountLimit the number of messages will be read in a single invocation.
     * @return the number of messages that have been processed.
     */
    int controlledRead(ControlledMessageHandler handler, int messageCountLimit);

    /**
     * The maximum message length in bytes supported by the underlying ring buffer.
     *
     * @return the maximum message length in bytes supported by the underlying ring buffer.
     */
    int maxMsgLength();

    /**
     * Get the next value that can be used for a correlation id on a message when a response needs to be correlated.
     * <p>
     * This method should be thread safe.
     *
     * @return the next value in the correlation sequence.
     */
    long nextCorrelationId();

    /**
     * Get the underlying buffer used by the RingBuffer for storage.
     *
     * @return the underlying buffer used by the RingBuffer for storage.
     */
    AtomicBuffer buffer();

    /**
     * Set the time of the last consumer heartbeat.
     * <p>
     * <b>Note:</b> The value for time must be valid across processes which means {@link System#nanoTime()}
     * is not a valid option.
     *
     * @param time of the last consumer heartbeat.
     */
    void consumerHeartbeatTime(long time);

    /**
     * The time of the last consumer heartbeat.
     *
     * @return the time of the last consumer heartbeat.
     */
    long consumerHeartbeatTime();

    /**
     * The position in bytes from start up of the producers. The figure includes the headers.
     * This is the range they are working with but could still be in the act of working with.
     *
     * @return number of bytes produced by the producers in claimed space.
     */
    long producerPosition();

    /**
     * The position in bytes from start up for the consumers. The figure includes the headers.
     *
     * @return the count of bytes consumed by the consumers.
     */
    long consumerPosition();

    /**
     * Size of the buffer backlog in bytes between producers and consumers. The value includes the size of headers.
     * <p>
     * This method gives a concurrent snapshot of the buffer whereby a concurrent read or write may be
     * partially complete and thus the value should be taken as an indication.
     *
     * @return size of the backlog of bytes in the buffer between producers and consumers.
     */
    int size();

    /**
     * Unblock a multi-producer ring buffer when a producer has died during the act of offering. The operation will
     * scan from the consumer position up to the producer position.
     * <p>
     * If no action is required at the position then none will be taken.
     *
     * @return true of an unblocking action was taken otherwise false.
     */
    boolean unblock();

    /**
     * Remaining free capacity in bytes that can still be claimed without the consumer having to advance.
     * <p>
     * Because at most one record header of padding is reserved when a record would wrap, a producer must leave
     * {@link RecordDescriptor#HEADER_LENGTH} bytes of headroom; values returned here are therefore clamped at
     * {@code capacity - HEADER_LENGTH}. The value is a concurrent snapshot and only an indication.
     *
     * @return number of free bytes currently available in the ring-buffer.
     */
    default int remainingCapacity()
    {
        return Math.max(0, capacity() - size() - RecordDescriptor.HEADER_LENGTH);
    }

    /**
     * Determine whether the buffered backlog has reached or crossed a high water mark expressed as a fraction of
     * the ring-buffer capacity. Producers can use this to throttle themselves before the buffer becomes full,
     * i.e. before any back-pressure wait is required.
     *
     * @param highWatermarkRatio fraction of capacity at which the producer should slow down, in the range
     *                           {@code [0.0, 1.0]}.
     * @return true when {@link #size()} is at least {@code highWatermarkRatio * capacity()}.
     * @throws IllegalArgumentException if the ratio is outside of the {@code [0.0, 1.0]} range.
     */
    default boolean isAtHighWatermark(final double highWatermarkRatio)
    {
        if (highWatermarkRatio < 0.0 || highWatermarkRatio > 1.0)
        {
            throw new IllegalArgumentException(
                "highWatermarkRatio must be in the [0.0, 1.0] range: " + highWatermarkRatio);
        }

        return size() >= (long)(capacity() * highWatermarkRatio);
    }

    /**
     * Determine whether the consumer is making progress, allowing a producer to fail fast instead of waiting out
     * the full back-pressure timeout when a consumer has crashed or is being restarted.
     * <p>
     * The check uses the last consumer heartbeat set via {@link #consumerHeartbeatTime(long)}; the supplied time
     * must come from a clock that is valid across processes, never {@link System#nanoTime()}. A heartbeat older
     * than {@code maxHeartbeatAge} means the consumer is considered stalled; callers that also want to distinguish
     * "slow but alive" from "dead" can additionally poll {@link #consumerPosition()} and treat a moving position
     * as progress. A zero heartbeat means no heartbeat has ever been published and liveness is unknown, in which
     * case this method reports the consumer as alive so callers never fail spuriously.
     *
     * @param nowMs           current wall clock time in milliseconds.
     * @param maxHeartbeatAge maximum tolerated heartbeat age in milliseconds.
     * @return true if the consumer is alive or its liveness cannot be determined, false if it appears stalled.
     */
    default boolean isConsumerAlive(final long nowMs, final long maxHeartbeatAge)
    {
        final long heartbeatTime = consumerHeartbeatTime();
        if (0L == heartbeatTime)
        {
            return true;
        }

        return nowMs - heartbeatTime <= maxHeartbeatAge;
    }
}
