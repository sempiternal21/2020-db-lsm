package ru.mail.polis.sempiternal21;

import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

final class SSTable implements Table {

    private final FileChannel channel;
    private final int numRows;
    private final long sizeData;

    SSTable(@NotNull final File file) throws IOException {
        channel = FileChannel.open(file.toPath(), StandardOpenOption.READ);
        final long sizeFile = channel.size();
        final ByteBuffer buf = ByteBuffer.allocate(Integer.BYTES);
        channel.read(buf, sizeFile - Integer.BYTES);
        numRows = buf.rewind().getInt();
        sizeData = sizeFile - (numRows + 1) * Integer.BYTES;
    }

    private int getOffset(final int numRow) throws IOException {
        final ByteBuffer buf = ByteBuffer.allocate(Integer.BYTES);
        channel.read(buf, sizeData + numRow * Integer.BYTES);
        return buf.rewind().getInt();
    }

    @NotNull
    private ByteBuffer key(final int row) throws IOException {
        final int offset = getOffset(row);
        final ByteBuffer keySize = ByteBuffer.allocate(Integer.BYTES);
        channel.read(keySize, offset);
        final ByteBuffer key = ByteBuffer.allocate(keySize.rewind().getInt());
        channel.read(key, offset + Integer.BYTES);
        return key.rewind();
    }

    @NotNull
    private Cell cell(final int row) throws IOException {
        int offset = getOffset(row);
        final ByteBuffer key = key(row);
        offset += key.remaining() + Integer.BYTES;
        final ByteBuffer timestamp = ByteBuffer.allocate(Long.BYTES);
        channel.read(timestamp, offset);
        offset += Long.BYTES;
        long bufferOffset = timestamp.rewind().getLong();
        if (bufferOffset < 0) {
            return new Cell(key, new Value(-bufferOffset));
        } else {
            final ByteBuffer valueSize = ByteBuffer.allocate(Integer.BYTES);
            channel.read(valueSize, offset);
            final ByteBuffer value = ByteBuffer.allocate(valueSize.rewind().getInt());
            offset += Integer.BYTES;
            channel.read(value, offset);
            return new Cell(key, new Value(timestamp.rewind().getLong(), value.rewind()));
        }
    }

    private int binarySearch(@NotNull final ByteBuffer from) throws IOException {
        int l = 0;
        int r = numRows - 1;
        while (l <= r) {
            final int med = (l + r) / 2;
            final int cmp = key(med).compareTo(from);
            if (cmp < 0) {
                l = med + 1;
            } else if (cmp > 0) {
                r = med - 1;
            } else {
                return med;
            }
        }
        return l;
    }

    @NotNull
    @Override
    public Iterator<Cell> iterator(@NotNull final ByteBuffer from) throws IOException {
        return new Iterator<>() {
            int pos = binarySearch(from);

            @Override
            public boolean hasNext() {
                return pos < numRows;
            }

            @Override
            public Cell next() {
                try {
                    return cell(pos++);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) {
        throw new UnsupportedOperationException("Immutable");
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) {
        throw new UnsupportedOperationException("Immutable");
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }

    /**
     * Structure of table.
     * keySize(Integer)|key|timestamp(Long)|tombstone(Byte)||valueSize(Integer)|value||
     * offsets
     * n
     */
    static void serialize(final File file, final Iterator<Cell> iterator) throws IOException {
        try (FileChannel fileChannel = new FileOutputStream(file).getChannel()) {
            final List<Integer> offsets = new ArrayList<>();
            int offset = 0;
            while (iterator.hasNext()) {
                offsets.add(offset);
                final Cell buf = iterator.next();
                final ByteBuffer key = buf.getKey();
                final Value value = buf.getValue();
                final int keySize = key.remaining();

                fileChannel.write(ByteBuffer.allocate(Integer.BYTES)
                        .putInt(keySize)
                        .rewind());
                fileChannel.write(key);
                if (value.isTombstone()) {
                    fileChannel.write(ByteBuffer.allocate(Long.BYTES)
                            .putLong(-value.getTimestamp())
                            .rewind());
                } else {
                    fileChannel.write(ByteBuffer.allocate(Long.BYTES)
                            .putLong(value.getTimestamp())
                            .rewind());
                    final ByteBuffer data = value.getData();
                    final int valueSize = data.remaining();
                    offset += Integer.BYTES + valueSize;
                    fileChannel.write(ByteBuffer.allocate(Integer.BYTES)
                            .putInt(valueSize)
                            .rewind());
                    fileChannel.write(data);
                }
                offset += Integer.BYTES + keySize + Long.BYTES;
            }

            for (final Integer off : offsets) {
                fileChannel.write(ByteBuffer.allocate(Integer.BYTES)
                        .putInt(off)
                        .rewind());
            }
            final int offsetSize = offsets.size();
            fileChannel.write(ByteBuffer.allocate(Integer.BYTES)
                    .putInt(offsetSize)
                    .rewind());
        }
    }
}
