package org.apache.directmemory.memory;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;

import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class MemoryManagerServiceImplTest {

    protected static final Random R = new Random();

    protected static final byte[] SMALL_PAYLOAD = "ABCD".getBytes();

    @Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{{MemoryManagerImpl.class}});
    }

    private final Class<? extends MemoryManager<Object>> memoryManagerServiceClass;

    public MemoryManagerServiceImplTest(Class<? extends MemoryManager<Object>> memoryManagerServiceClass) {
        this.memoryManagerServiceClass = memoryManagerServiceClass;
    }

    protected MemoryManager<Object> getMemoryManagerService() {
        try {
            return memoryManagerServiceClass.newInstance();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testFirstMatchBorderCase()
            throws IOException {

        // Storing a first payload of 4 bytes, 1 byte remaining in the buffer.
        // When storing a second 4 bytes payload, an BufferOverflowException is
        // thrown instead of an easy check.

        final int BUFFER_SIZE = 5;

        final MemoryManager<Object> memoryManager = getMemoryManagerService();

        memoryManager.init(1, BUFFER_SIZE);

        Pointer<Object> pointer1 = memoryManager.store(SMALL_PAYLOAD);
        assertNotNull(pointer1);

        Pointer<Object> pointer2 = memoryManager.store(SMALL_PAYLOAD);
        assertNull(pointer2);

        memoryManager.close();
    }

    @Test
    public void testAllocateMultipleBuffers()
            throws IOException {

        // Initializing 4 buffers of 4 bytes, MemoryManager should search
        // for available space in another buffer.

        final int NUMBER_OF_OBJECTS = 4;

        final MemoryManager<Object> memoryManager = getMemoryManagerService();

        memoryManager.init(NUMBER_OF_OBJECTS, SMALL_PAYLOAD.length);

        for (int i = 0; i < NUMBER_OF_OBJECTS; i++) {
            Pointer<Object> pointer = memoryManager.store(SMALL_PAYLOAD);
            assertNotNull(pointer);
        }

        Pointer<Object> pointerNull = memoryManager.store(SMALL_PAYLOAD);
        assertNull(pointerNull);

        memoryManager.close();
    }

    @Test
    public void testByteLeaking()
            throws IOException {

        // Initializing 1 buffer of 10*4 bytes, should be able to allocate 10
        // objects of 4 bytes.

        final int NUMBER_OF_OBJECTS = 10;

        final MemoryManager<Object> memoryManager = getMemoryManagerService();
        memoryManager.init(1, NUMBER_OF_OBJECTS * SMALL_PAYLOAD.length);

        for (int i = 0; i < NUMBER_OF_OBJECTS; i++) {
            Pointer<Object> pointer = memoryManager.store(SMALL_PAYLOAD);
            assertNotNull(pointer);
        }

        Pointer<Object> pointerNull = memoryManager.store(SMALL_PAYLOAD);
        assertNull(pointerNull);

        memoryManager.close();
    }

    @Test
    public void testReportCorrectUsedMemory()
            throws IOException {

        // Initializing 1 buffer of 4*4 bytes, storing and freeing and storing
        // again should report correct numbers.

        final int NUMBER_OF_OBJECTS = 4;
        final int BUFFER_SIZE = NUMBER_OF_OBJECTS * SMALL_PAYLOAD.length;

        final MemoryManager<Object> memoryManager = getMemoryManagerService();

        memoryManager.init(1, BUFFER_SIZE);

        Pointer<Object> lastPointer = null;
        for (int i = 0; i < NUMBER_OF_OBJECTS; i++) {
            Pointer<Object> pointer = memoryManager.store(SMALL_PAYLOAD);
            assertNotNull(pointer);
            lastPointer = pointer;
        }

        // Buffer is fully used.
        assertEquals(BUFFER_SIZE, memoryManager.used());

        assertNotNull(lastPointer);
        memoryManager.free(lastPointer);

        Pointer<Object> pointerNotNull = memoryManager.store(SMALL_PAYLOAD);
        assertNotNull(pointerNotNull);

        // Buffer again fully used.
        assertEquals(BUFFER_SIZE, memoryManager.used());

        memoryManager.close();
    }

    @Test
    public void testRandomPayload()
            throws IOException {

        final int NUMBER_OF_OBJECTS = 10;
        final int BUFFER_SIZE = NUMBER_OF_OBJECTS * SMALL_PAYLOAD.length;

        final MemoryManager<Object> memoryManager = getMemoryManagerService();

        memoryManager.init(1, BUFFER_SIZE);

        for (int i = 0; i < NUMBER_OF_OBJECTS; i++) {
            byte[] payload = MemoryTestUtils.generateRandomPayload(SMALL_PAYLOAD.length);
            Pointer<Object> pointer = memoryManager.store(payload);
            assertNotNull(pointer);
            byte[] fetchedPayload = memoryManager.retrieve(pointer);
            assertEquals(new String(payload), new String(fetchedPayload));
            if (R.nextBoolean()) {
                memoryManager.free(pointer);
            }
        }

        memoryManager.clear();

        for (int i = 0; i < NUMBER_OF_OBJECTS; i++) {
            byte[] payload = MemoryTestUtils.generateRandomPayload(SMALL_PAYLOAD.length);
            Pointer<Object> pointer = memoryManager.store(payload);
            assertNotNull(pointer);
            byte[] fetchedPayload = memoryManager.retrieve(pointer);
            assertEquals(new String(payload), new String(fetchedPayload));
            if (R.nextBoolean()) {
                memoryManager.free(pointer);
                i--;
            }
        }

        memoryManager.clear();

        Pointer<Object> pointer = null;
        do {
            byte[] payload = MemoryTestUtils.generateRandomPayload(R.nextInt(BUFFER_SIZE / 4 + 1));
            pointer = memoryManager.store(payload);
            if (pointer != null && R.nextBoolean()) {
                memoryManager.free(pointer);
            }
        }
        while (pointer != null);

        memoryManager.close();
    }

}
