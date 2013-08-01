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

import org.apache.directmemory.measures.Ram;
import org.apache.directmemory.memory.buffer.MemoryBuffer;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@Ignore
public class NIOTest {

    private static Logger logger = LoggerFactory.getLogger(NIOTest.class);
    private MemoryManager<Object> memoryManager;

    @Before
    public static void init() {
        byte[] payload = "012345678901234567890123456789012345678901234567890123456789".getBytes();

        logger.info("init");
        MemoryManager<Object> memoryManager = new MemoryManagerImpl<Object>();
        memoryManager.init(1, Ram.Mb(100));

        logger.info("payload size=" + Ram.inKb(payload.length));
        long howMany = (memoryManager.capacity() / payload.length);
        howMany = (howMany * 50) / 100;

        for (int i = 0; i < howMany; i++) {
            Pointer<Object> p = memoryManager.store(payload);
            assertNotNull(p);
        }

        logger.info("" + howMany + " items stored");
    }

    @Test
    public void nioTest() {
        Random rnd = new Random();
        int size = rnd.nextInt(10) * (int) memoryManager.capacity() / 100;
        logger.info("payload size=" + Ram.inKb(size));
        Pointer<Object> p = memoryManager.allocate(Object.class,size);
        MemoryBuffer b = p.getMemoryBuffer();
        logger.info("allocated");
        assertNotNull(p);
        assertNotNull(b);

        // assertTrue( b.isDirect() );
        assertEquals(0, b.readerIndex());
        assertEquals(size, b.capacity());

        byte[] check = memoryManager.retrieve(p);

        assertNotNull(check);

        assertEquals(size, p.getCapacity());
        logger.info("end");
    }

    @After
    public void cleanup() throws IOException {
        memoryManager.close();
    }
}
