package com.simplekafka.broker;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PartitionTest {
    @TempDir Path temporaryDirectory;

    @Test
    void appendsAndReadsMessagesByOffsetAfterRestart() throws Exception {
        Path directory = temporaryDirectory.resolve("partition-0");
        try (Partition partition = new Partition(0, directory)) {
            assertEquals(0, partition.append(bytes("first")));
            assertEquals(1, partition.append(bytes("second")));
            assertEquals(2, partition.getNextOffset());
        }

        try (Partition recovered = new Partition(0, directory)) {
            assertEquals(2, recovered.getNextOffset());
            assertMessages(recovered.readMessages(1, 1024), "second");
            assertEquals(2, Files.size(directory.resolve("00000000000000000000.index")) / 16);
        }
    }

    @Test
    void rotatesSegmentsAndReadsAcrossTheirBoundary() throws Exception {
        Path directory = temporaryDirectory.resolve("rotated");
        try (Partition partition = new Partition(4, directory, 10)) {
            assertEquals(0, partition.append(bytes("one")));
            assertEquals(1, partition.append(bytes("two")));
            assertEquals(2, partition.append(bytes("three")));
            assertEquals(3, partition.getSegmentCount());
            assertMessages(partition.readMessages(0, 1024), "one", "two", "three");
            assertMessages(partition.readMessages(1, 9), "two");
        }
    }

    @Test
    void boundsReadsButReturnsOneOversizedFirstMessageToAllowProgress() throws Exception {
        try (Partition partition = new Partition(0, temporaryDirectory.resolve("bounds"))) {
            partition.append(bytes("abc"));
            partition.append(bytes("defg"));
            partition.append(bytes("h"));
            assertMessages(partition.readMessages(0, 7), "abc");
            assertMessages(partition.readMessages(1, 1), "defg");
            assertMessages(partition.readMessages(3, 50));
            assertThrows(IllegalArgumentException.class, () -> partition.readMessages(-1, 10));
            assertThrows(IllegalArgumentException.class, () -> partition.readMessages(0, 0));
        }
    }

    @Test
    void recoversOffsetsRebuildsIndexAndDropsAnIncompleteTail() throws Exception {
        Path directory = temporaryDirectory.resolve("recovery");
        Path log = directory.resolve("00000000000000000000.log");
        Path index = directory.resolve("00000000000000000000.index");
        try (Partition partition = new Partition(0, directory)) {
            partition.append(bytes("kept"));
        }
        try (FileChannel channel = FileChannel.open(log, StandardOpenOption.WRITE, StandardOpenOption.APPEND)) {
            channel.write(ByteBuffer.wrap(new byte[] {0, 0}));
        }
        Files.write(index, new byte[] {1, 2, 3}, StandardOpenOption.TRUNCATE_EXISTING);

        try (Partition recovered = new Partition(0, directory)) {
            assertEquals(1, recovered.getNextOffset());
            assertMessages(recovered.readMessages(0, 100), "kept");
            assertEquals(16, Files.size(index));
            assertEquals(8, Files.size(log));
            assertEquals(1, recovered.append(bytes("next")));
        }
    }

    @Test
    void rejectsBadConstructorAndMessageInputs() throws Exception {
        assertThrows(IllegalArgumentException.class, () -> new Partition(-1, temporaryDirectory.resolve("bad")));
        assertThrows(IllegalArgumentException.class, () -> new Partition(0, temporaryDirectory.resolve("small"), 4));
        try (Partition partition = new Partition(0, temporaryDirectory.resolve("valid"))) {
            assertThrows(IllegalArgumentException.class, () -> partition.append(null));
        }
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static void assertMessages(List<byte[]> actual, String... expected) {
        assertEquals(expected.length, actual.size());
        for (int i = 0; i < expected.length; i++) {
            assertArrayEquals(bytes(expected[i]), actual.get(i), "message " + i);
        }
    }
}
