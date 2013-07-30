package org.apache.directmemory.serialization;

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

import com.carrotsearch.junitbenchmarks.AbstractBenchmark;
import com.carrotsearch.junitbenchmarks.BenchmarkOptions;
import com.carrotsearch.junitbenchmarks.annotation.AxisRange;
import com.carrotsearch.junitbenchmarks.annotation.BenchmarkHistoryChart;
import com.carrotsearch.junitbenchmarks.annotation.BenchmarkMethodChart;
import com.carrotsearch.junitbenchmarks.annotation.LabelType;
import org.apache.directmemory.measures.Monitor;
import org.apache.directmemory.measures.MonitorService;
import org.apache.directmemory.measures.Ram;
import org.apache.directmemory.misc.DummyPojo;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@AxisRange(min = 0, max = 1)
@BenchmarkMethodChart()
@BenchmarkHistoryChart(labelWith = LabelType.CUSTOM_KEY, maxRuns = 5)
@BenchmarkOptions(benchmarkRounds = 2, warmupRounds = 1, concurrency = 1)

public class SerializerTest
        extends AbstractBenchmark {

    private static Logger logger = LoggerFactory.getLogger(SerializerTest.class);

    private void testSerializer(String name, Serializer serializer, int size, int howMany)
            throws IOException, ClassNotFoundException, InstantiationException, IllegalAccessException {
        logger.info("begin " + serializer.getClass().toString());
        MonitorService stopWatch = Monitor.get("serializer." + name + "." + size + "bytes");
        MonitorService stopWatch2 = Monitor.get("deserializer." + name + "." + size + "bytes");
        DummyPojo pojo = new DummyPojo("test", size);
        for (int i = 0; i < howMany; i++) {
            long split = stopWatch.start();
            final byte[] array = serializer.serialize(pojo);
            stopWatch.stop(split);
            long split2 = stopWatch2.start();
            DummyPojo check = (DummyPojo) serializer.deserialize(array, pojo.getClass());
            stopWatch2.stop(split2);
            assertNotNull("object has not been serialized", check);
            assertEquals(pojo.name, check.name);
        }
        logger.info("end serialize " + serializer.getClass().toString() + "\r\n" + stopWatch.toString());
        logger.info("end deserialize " + serializer.getClass().toString() + "\r\n" + stopWatch2.toString());
    }

    @Test
    public void StandardTest()
            throws IOException, ClassNotFoundException, InstantiationException, IllegalAccessException {
        testSerializer("java-serialization", new StandardSerializer(), Ram.Kb(1), 20000);
        testSerializer("java-serialization", new StandardSerializer(), Ram.Kb(2), 20000);
        testSerializer("java-serialization", new StandardSerializer(), Ram.Kb(3), 20000);
        testSerializer("java-serialization", new StandardSerializer(), Ram.Kb(4), 20000);
    }

}
