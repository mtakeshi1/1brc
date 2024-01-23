/*
 *  Copyright 2023 The original authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package dev.morling.onebrc;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collector;

public class CalculateAverage_mtakeshi {

    private static final String FILE = "./measurements.txt";

    private static final int PIECE_LEN = 40 * 1024 * 1024;

    private record Measurement(String station, double value) {
        private Measurement(String[] parts) {
            this(parts[0], Double.parseDouble(parts[1]));
        }
    }

    private record ResultRow(double min, double mean, double max) {

        public String toString() {
            return STR."\{round(min)}/\{round(mean)}/\{round(max)}";
        }

        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }
    }

    private static class MeasurementAggregator {

        public MeasurementAggregator() {
        }

        public MeasurementAggregator(double v) {
            this.min = v;
            this.max = v;
            this.sum = v;
            this.count = 1;
        }

        private double min = Double.POSITIVE_INFINITY;
        private double max = Double.NEGATIVE_INFINITY;
        private double sum;
        private long count;

        public MeasurementAggregator combine(MeasurementAggregator measurementAggregator) {
            var res = new MeasurementAggregator();
            res.min = Math.min(this.min, measurementAggregator.min);
            res.max = Math.max(this.max, measurementAggregator.max);
            res.sum = this.sum + measurementAggregator.sum;
            res.count = this.count + measurementAggregator.count;

            return res;
        }

        public void append(double x) {
            this.min = Math.min(this.min, x);
            this.max = Math.max(this.max, x);
            this.sum += x;
            this.count++;
        }
    }

    public static void main(String[] args) throws IOException {
        Collector<Measurement, MeasurementAggregator, ResultRow> collector = Collector.of(
                MeasurementAggregator::new,
                (a, m) -> {
                    a.min = Math.min(a.min, m.value);
                    a.max = Math.max(a.max, m.value);
                    a.sum += m.value;
                    a.count++;
                },
                (agg1, agg2) -> {
                    var res = new MeasurementAggregator();
                    res.min = Math.min(agg1.min, agg2.min);
                    res.max = Math.max(agg1.max, agg2.max);
                    res.sum = agg1.sum + agg2.sum;
                    res.count = agg1.count + agg2.count;

                    return res;
                },
                agg -> new ResultRow(agg.min, (Math.round(agg.sum * 10.0) / 10.0) / agg.count, agg.max));

        var file = args.length == 0 ? FILE : args[0];

        try (var raf = new RandomAccessFile(file, "r"); ExecutorService executor = Executors.newFixedThreadPool(16)) {
            FileChannel channel = raf.getChannel();
            // channel.ma
            long lastOffset = 0;
            long offset = Math.min(PIECE_LEN, raf.length());
            var seg = channel.map(FileChannel.MapMode.READ_ONLY, 0, raf.length(), Arena.global());
            var futures = new ArrayList<Future<Map<String, MeasurementAggregator>>>(100000); // yes lazy
            while (offset < raf.length()) {
                while (offset < raf.length() && seg.get(ValueLayout.OfByte.JAVA_BYTE, offset) != '\n') {
                    offset++;
                }
                var piece = seg.asSlice(lastOffset, offset - lastOffset);
                lastOffset = offset;
                offset = Math.min(offset + PIECE_LEN, raf.length());
                futures.add(submitTask(executor, piece));
            }
            if (offset > lastOffset) {
                futures.add(submitTask(executor, seg.asSlice(lastOffset, offset - lastOffset)));
            }
            Map<String, MeasurementAggregator> finalResult = new HashMap<>(100000);
            futures.stream().parallel().map(CalculateAverage_mtakeshi::waitRethrow).forEach(map -> map.forEach((station, result) -> {
                finalResult.merge(station, result, MeasurementAggregator::combine);
            }));
            Map<String, ResultRow> measurements = new TreeMap<>();
            finalResult.forEach((k, v) -> {
                measurements.put(k.trim(), new ResultRow(v.min, (Math.round(v.sum * 10.0) / 10.0) / v.count, v.max));
            });
            System.out.println(measurements);
        }
    }

    private static <E> E waitRethrow(Future<E> future) {
        try {
            return future.get();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static Map<String, MeasurementAggregator> task(MemorySegment piece) {
        Map<String, MeasurementAggregator> map = new HashMap<>();
        long i = 0;
        byte[] buffer = new byte[100];
        while (i < piece.byteSize()) {
            byte b;
            int j = 0;
            while ((b = piece.get(ValueLayout.JAVA_BYTE, i++)) != ';') {
                if (b != '\n') buffer[j++] = b;
            }
            int sig = 1;
            double v = 0;
            if ((b = piece.get(ValueLayout.JAVA_BYTE, i++)) == '-') {
                sig = -1;
            } else {
                v = b - '0';
            }
            while ((b = piece.get(ValueLayout.JAVA_BYTE, i++)) != '.') {
                int digit = b - '0';
                v = (v * 10) + digit;
            }
            b = piece.get(ValueLayout.JAVA_BYTE, i++);
            int digit = b - '0';
            if (digit > 0) {
                v += digit / 10.0;
            }
            map.merge(new String(buffer, 0, j), new MeasurementAggregator(sig * v), MeasurementAggregator::combine);
            if (i < piece.byteSize()) {
                if ((b = piece.get(ValueLayout.JAVA_BYTE, i++)) != '\n') {
                    throw new RuntimeException(STR."expected EOL but got '\{Character.toString(b)}' ");
                }
            }
        }
        return map;
    }

    private static Future<Map<String, MeasurementAggregator>> submitTask(ExecutorService executor, MemorySegment piece) {
        return executor.submit(() -> task(piece));
    }
}
