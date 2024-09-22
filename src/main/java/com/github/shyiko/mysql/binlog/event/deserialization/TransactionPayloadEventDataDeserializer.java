/*
 * Copyright 2013 Stanley Shyiko
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.shyiko.mysql.binlog.event.deserialization;

import com.github.luben.zstd.Zstd;
import com.github.luben.zstd.ZstdInputStream;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.TransactionPayloadEventData;
import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;

/**
 * @author <a href="mailto:somesh.malviya@booking.com">Somesh Malviya</a>
 * @author <a href="mailto:debjeet.sarkar@booking.com">Debjeet Sarkar</a>
 */
public class TransactionPayloadEventDataDeserializer implements EventDataDeserializer<TransactionPayloadEventData> {
    public static final int OTW_PAYLOAD_HEADER_END_MARK = 0;
    public static final int OTW_PAYLOAD_SIZE_FIELD = 1;
    public static final int OTW_PAYLOAD_COMPRESSION_TYPE_FIELD = 2;
    public static final int OTW_PAYLOAD_UNCOMPRESSED_SIZE_FIELD = 3;
    private static final int BUFFER_SIZE = 1024;

    @Override
    public TransactionPayloadEventData deserialize(ByteArrayInputStream inputStream) throws IOException {
        TransactionPayloadEventData eventData = new TransactionPayloadEventData();
        boolean chunked = false;
        long uncompressedSize = 0;
        // Read the header fields from the event data
        while (inputStream.available() > 0) {
            int fieldType = 0;
            int fieldLen = 0;
            // Read the type of the field
            if (inputStream.available() >= 1) {
                fieldType = inputStream.readPackedInteger();
            }
            // We have reached the end of the Event Data Header
            if (fieldType == OTW_PAYLOAD_HEADER_END_MARK) {
                break;
            }
            // Read the size of the field
            if (inputStream.available() >= 1) {
                fieldLen = inputStream.readPackedInteger();
            }
            switch (fieldType) {
                case OTW_PAYLOAD_SIZE_FIELD:
                    // Fetch the payload size
                    eventData.setPayloadSize(inputStream.readPackedInteger());
                    break;
                case OTW_PAYLOAD_COMPRESSION_TYPE_FIELD:
                    // Fetch the compression type
                    eventData.setCompressionType(inputStream.readPackedInteger());
                    break;
                case OTW_PAYLOAD_UNCOMPRESSED_SIZE_FIELD:
                    // Fetch the uncompressed size
                    uncompressedSize = inputStream.readPackedLong();
                    if (uncompressedSize > Integer.MAX_VALUE)
                        eventData.setUncompressedSize(Integer.MAX_VALUE);
                    else
                        eventData.setUncompressedSize((int)uncompressedSize);
                    break;
                default:
                    // Ignore unrecognized field
                    inputStream.read(fieldLen);
                    break;
            }
        }
        if (eventData.getUncompressedSize() == 0) {
            // Default the uncompressed to the payload size
            eventData.setUncompressedSize(eventData.getPayloadSize());
        }
        // set the payload to the rest of the input buffer
        eventData.setPayload(inputStream.read(eventData.getPayloadSize()));

        ArrayList<Event> decompressedEvents = new ArrayList<>();
//        if (chunked) {
            // Decompress the payload
        decompressedEvents = getUncompressedEvents(eventData);
//        }
//        else {
//            // Decompress the payload
//            byte[] src = eventData.getPayload();
//            byte[] dst = ByteBuffer.allocate((int)eventData.getUncompressedSize()).array();
//            Zstd.decompressByteArray(dst, 0, dst.length, src, 0, src.length);
//
//            // Read and store events from decompressed byte array into input stream
//            EventDeserializer transactionPayloadEventDeserializer = new EventDeserializer();
//            ByteArrayInputStream destinationInputStream = new ByteArrayInputStream(dst);
//
//            Event internalEvent = transactionPayloadEventDeserializer.nextEvent(destinationInputStream);
//            while (internalEvent != null) {
//                decompressedEvents.add(internalEvent);
//                internalEvent = transactionPayloadEventDeserializer.nextEvent(destinationInputStream);
//            }
//        }
        eventData.setUncompressedEvents(decompressedEvents);
        return eventData;
    }

     // Buffer size (can be adjusted)

    /**
     * Decompresses the payload of a TransactionPayloadEvent and deserializes the
     * contained events.
     *
     * @param eventData the TransactionPayloadEvent containing the payload
     * @param length the length of the compressed payload
     * @return the list of deserialized events
     * @throws IOException if an error occurs during decompression or deserialization
     */
    public ArrayList<Event> getUncompressedEvents(TransactionPayloadEventData eventData) throws IOException {
        byte[] src = eventData.getPayload();
        ArrayList<Event> decompressedEvents = new ArrayList<>();

        // Wrap the input payload in a ZstdInputStream for decompression
        try (ByteArrayInputStream inputStream = new ByteArrayInputStream(src);
             ZstdInputStream zstdInputStream = new ZstdInputStream(inputStream)) {

            // Use a buffer to read decompressed data in chunks
            byte[] buffer = new byte[BUFFER_SIZE];
            int bytesRead;
            ByteArrayOutputStream bufferStream = new ByteArrayOutputStream();
            EventDeserializer eventDeserializer = new EventDeserializer();
            ByteArrayInputStream eventStream;
            byte[] leftOver = new byte[0];
            while ((bytesRead = zstdInputStream.read(buffer)) != -1) {
                // Combine the current chunk with any leftover data from the previous chunk
                byte[] combined = new byte[bytesRead + leftOver.length];
                System.arraycopy(leftOver, 0, combined, 0, leftOver.length);
                System.arraycopy(buffer, 0, combined, leftOver.length, bytesRead);

                // Write the decompressed chunk to the buffer stream for deserialization
                bufferStream.reset();
                bufferStream.write(combined);

                // Convert buffer stream to ByteArrayInputStream for deserialization
                eventStream = new ByteArrayInputStream(bufferStream.toByteArray());
                try {
                    Event event = eventDeserializer.nextEvent(eventStream);
                    // Deserialize events from the stream
                    while (event != null) {
                        decompressedEvents.add(event);
                        eventStream.mark(BUFFER_SIZE);
                        event = eventDeserializer.nextEvent(eventStream);
                    }
                }
                catch (IOException e) {
                    System.out.println("cant read data" + e);
                }

                // Reset buffer stream after deserializing the current set of events
                eventStream.reset();
                leftOver = eventStream.read(eventStream.available());
            }
        }

        return decompressedEvents;
    }

//    public ArrayList<Event> getUncompressedEvents(TransactionPayloadEventData eventData, long length) throws IOException {
//        byte[] src = eventData.getPayload();
//        ArrayList<Event> decompressedEvents = new ArrayList<>();
//        java.io.ByteArrayInputStream inputStream = new java.io.ByteArrayInputStream(src);
//        // Create a piped output stream and piped input stream pair
//        PipedOutputStream pipedOutputStream = new PipedOutputStream();
//        try (PipedInputStream pipedInputStream = new PipedInputStream(pipedOutputStream)) {
//
//            EventDeserializer transactionPayloadEventDeserializer = new EventDeserializer();
//            ZstdInputStream zstdInputStream = new ZstdInputStream(inputStream);
//            OutputStream outputStream = pipedOutputStream;
//            byte[] buffer = new byte[2000]; // Buffer size of 8 KB
//            int bytesRead;
//            byte[] leftOver = new byte[0];
//
//            while ((bytesRead = zstdInputStream.read(buffer)) != -1) {
//                byte[] combined = new byte[buffer.length + leftOver.length];
//                int combine_length = buffer.length + leftOver.length;
//                System.arraycopy(leftOver, 0, combined, 0, leftOver.length);
//                System.arraycopy(buffer, 0, combined, leftOver.length, buffer.length);
//
//                outputStream.write(combined, 0, combine_length);
//                ByteArrayInputStream destinationInputStream = new ByteArrayInputStream(pipedInputStream);
//                System.out.println("Read " + bytesRead + " bytes");
//                Event internalEvent = transactionPayloadEventDeserializer.nextEvent(destinationInputStream);
//                int currentInternalEventPos = 0;
//                while (internalEvent != null) {
//                    decompressedEvents.add(internalEvent);
//                    if (destinationInputStream.getPosition() >= length) {
//                        break;
//                    }
//                    currentInternalEventPos = destinationInputStream.getPosition();
//                    internalEvent = transactionPayloadEventDeserializer.nextEvent(destinationInputStream);
//                }
//                if (destinationInputStream.available() > 0) {
//                    leftOver = new byte[bytesRead - currentInternalEventPos];
//                    System.arraycopy(buffer, currentInternalEventPos, leftOver, 0, bytesRead - currentInternalEventPos);
//                }
//                System.out.println("Done reading");
//            }
//            pipedOutputStream.close();
//        }
//        return decompressedEvents;
//    }
}
