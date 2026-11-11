package com.simplekafka.broker;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/** A durable, append-only message log split into offset-based segments. */
public final class Partition implements AutoCloseable {
    private static final int LENGTH_BYTES = Integer.BYTES;
    private static final int INDEX_ENTRY_BYTES = Long.BYTES * 2;
    private static final long DEFAULT_SEGMENT_BYTES = 1024L * 1024L;

    private final int id;
    private final Path directory;
    private final long maxSegmentBytes;
    private final List<Segment> segments = new ArrayList<>();
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private long nextOffset;
    private boolean closed;

    public Partition(int id, Path directory) throws IOException {
        this(id, directory, DEFAULT_SEGMENT_BYTES);
    }

    public Partition(int id, Path directory, long maxSegmentBytes) throws IOException {
        if (id < 0) throw new IllegalArgumentException("partition id must be non-negative");
        if (directory == null) throw new IllegalArgumentException("directory must not be null");
        if (maxSegmentBytes < LENGTH_BYTES + 1L) {
            throw new IllegalArgumentException("segment size must fit a length prefix and one byte");
        }
        this.id = id;
        this.directory = directory;
        this.maxSegmentBytes = maxSegmentBytes;
        initialize();
    }

    public int getId() {
        return id;
    }

    /** Appends one message and returns its zero-based partition offset. */
    public long append(byte[] message) throws IOException {
        if (message == null) throw new IllegalArgumentException("message must not be null");
        if (message.length > Integer.MAX_VALUE - LENGTH_BYTES) {
            throw new IllegalArgumentException("message is too large to encode");
        }
        lock.writeLock().lock();
        try {
            ensureOpen();
            long recordBytes = LENGTH_BYTES + (long) message.length;
            Segment active = segments.get(segments.size() - 1);
            if (active.log.size() > 0 && active.log.size() + recordBytes > maxSegmentBytes) {
                active = createSegment(nextOffset);
            }
            long position = active.log.size();
            ByteBuffer record = ByteBuffer.allocate((int) recordBytes);
            record.putInt(message.length).put(message).flip();
            long indexPosition = active.index.size();
            long offset = nextOffset;
            ByteBuffer indexEntry = ByteBuffer.allocate(INDEX_ENTRY_BYTES).putLong(offset).putLong(position);
            indexEntry.flip();
            try {
                writeFully(active.log, record, position);
                active.log.force(true);
                writeFully(active.index, indexEntry, indexPosition);
                active.index.force(true);
            } catch (IOException failure) {
                try {
                    active.log.truncate(position);
                    active.index.truncate(indexPosition);
                    active.log.force(true);
                    active.index.force(true);
                } catch (IOException rollbackFailure) {
                    failure.addSuppressed(rollbackFailure);
                }
                throw failure;
            }
            active.entries.add(new IndexEntry(offset, position));
            nextOffset++;
            return offset;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /** Reads in offset order up to maxBytes; the first record is returned even if it alone exceeds the limit. */
    public List<byte[]> readMessages(long offset, int maxBytes) throws IOException {
        if (offset < 0) throw new IllegalArgumentException("offset must be non-negative");
        if (maxBytes < 1) throw new IllegalArgumentException("maxBytes must be positive");
        lock.readLock().lock();
        try {
            ensureOpen();
            List<byte[]> result = new ArrayList<>();
            long returnedBytes = 0;
            for (Segment segment : segments) {
                int entryIndex = lowerBound(segment.entries, offset);
                if (entryIndex == segment.entries.size()) continue;
                try (FileChannel reader = FileChannel.open(segment.logPath, StandardOpenOption.READ)) {
                    for (int i = entryIndex; i < segment.entries.size(); i++) {
                        IndexEntry entry = segment.entries.get(i);
                        ByteBuffer length = ByteBuffer.allocate(LENGTH_BYTES);
                        readFully(reader, length, entry.position);
                        length.flip();
                        int messageLength = length.getInt();
                        if (messageLength < 0) throw new IOException("negative message length at offset " + entry.offset);
                        if (!result.isEmpty() && returnedBytes + LENGTH_BYTES + messageLength > maxBytes) {
                            return result;
                        }
                        ByteBuffer message = ByteBuffer.allocate(messageLength);
                        readFully(reader, message, entry.position + LENGTH_BYTES);
                        result.add(message.array());
                        returnedBytes += LENGTH_BYTES + messageLength;
                    }
                }
            }
            return result;
        } finally {
            lock.readLock().unlock();
        }
    }

    public long getNextOffset() {
        lock.readLock().lock();
        try {
            return nextOffset;
        } finally {
            lock.readLock().unlock();
        }
    }

    public int getSegmentCount() {
        lock.readLock().lock();
        try {
            return segments.size();
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void close() throws IOException {
        lock.writeLock().lock();
        try {
            if (closed) return;
            IOException failure = null;
            for (Segment segment : segments) {
                try {
                    segment.log.close();
                } catch (IOException error) {
                    failure = error;
                }
                try {
                    segment.index.close();
                } catch (IOException error) {
                    if (failure == null) failure = error;
                    else failure.addSuppressed(error);
                }
            }
            closed = true;
            if (failure != null) throw failure;
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void initialize() throws IOException {
        Files.createDirectories(directory);
        List<Path> logs = new ArrayList<>();
        try (DirectoryStream<Path> files = Files.newDirectoryStream(directory, "*.log")) {
            for (Path path : files) logs.add(path);
        }
        logs.sort(Comparator.comparingLong(Partition::baseOffset));
        for (Path logPath : logs) {
            long baseOffset = baseOffset(logPath);
            if (baseOffset != nextOffset) {
                throw new IOException("non-contiguous segment offsets at " + logPath.getFileName());
            }
            Segment segment = openSegment(logPath, baseOffset);
            recoverSegment(segment);
            segments.add(segment);
        }
        if (segments.isEmpty()) createSegment(0);
    }

    private void recoverSegment(Segment segment) throws IOException {
        long position = 0;
        long size = segment.log.size();
        try (FileChannel reader = FileChannel.open(segment.logPath, StandardOpenOption.READ)) {
            while (position < size) {
                if (size - position < LENGTH_BYTES) break;
                ByteBuffer length = ByteBuffer.allocate(LENGTH_BYTES);
                readFully(reader, length, position);
                length.flip();
                int messageLength = length.getInt();
                if (messageLength < 0) throw new IOException("negative message length in " + segment.logPath);
                long recordEnd = position + LENGTH_BYTES + (long) messageLength;
                if (recordEnd > size) break;
                long offset = nextOffset++;
                segment.entries.add(new IndexEntry(offset, position));
                position = recordEnd;
            }
        }
        if (position != size) segment.log.truncate(position);
        segment.index.truncate(0);
        long indexPosition = 0;
        for (IndexEntry entry : segment.entries) {
            ByteBuffer bytes = ByteBuffer.allocate(INDEX_ENTRY_BYTES).putLong(entry.offset).putLong(entry.position);
            bytes.flip();
            writeFully(segment.index, bytes, indexPosition);
            indexPosition += INDEX_ENTRY_BYTES;
        }
        segment.log.force(true);
        segment.index.force(true);
    }

    private Segment createSegment(long baseOffset) throws IOException {
        Path logPath = directory.resolve(String.format("%020d.log", baseOffset));
        Segment segment = openSegment(logPath, baseOffset);
        segment.log.truncate(0);
        segment.index.truncate(0);
        segments.add(segment);
        return segment;
    }

    private Segment openSegment(Path logPath, long baseOffset) throws IOException {
        Path indexPath = directory.resolve(String.format("%020d.index", baseOffset));
        FileChannel log = FileChannel.open(logPath, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        try {
            FileChannel index = FileChannel.open(indexPath, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
            return new Segment(logPath, log, index);
        } catch (IOException error) {
            log.close();
            throw error;
        }
    }

    private static long baseOffset(Path logPath) {
        String name = logPath.getFileName().toString();
        return Long.parseLong(name.substring(0, name.length() - 4));
    }

    private static int lowerBound(List<IndexEntry> entries, long offset) {
        int low = 0;
        int high = entries.size();
        while (low < high) {
            int middle = (low + high) >>> 1;
            if (entries.get(middle).offset < offset) low = middle + 1;
            else high = middle;
        }
        return low;
    }

    private static void writeFully(FileChannel channel, ByteBuffer buffer, long position) throws IOException {
        while (buffer.hasRemaining()) position += channel.write(buffer, position);
    }

    private static void readFully(FileChannel channel, ByteBuffer buffer, long position) throws IOException {
        while (buffer.hasRemaining()) {
            int read = channel.read(buffer, position);
            if (read < 0) throw new IOException("unexpected end of partition log");
            position += read;
        }
    }

    private void ensureOpen() throws IOException {
        if (closed) throw new IOException("partition is closed");
    }

    private static final class Segment {
        private final Path logPath;
        private final FileChannel log;
        private final FileChannel index;
        private final List<IndexEntry> entries = new ArrayList<>();

        private Segment(Path logPath, FileChannel log, FileChannel index) {
            this.logPath = logPath;
            this.log = log;
            this.index = index;
        }
    }

    private static final class IndexEntry {
        private final long offset;
        private final long position;

        private IndexEntry(long offset, long position) {
            this.offset = offset;
            this.position = position;
        }
    }
}
