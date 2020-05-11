package ru.mail.polis.sempiternal21;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.TreeMap;

final class MemTable implements Table {

    private final SortedMap<ByteBuffer, Value> map = new TreeMap<>();
    private long sizeInBytes;

    MemTable() {
        sizeInBytes = 720L;
    }

    @NotNull
    @Override
    public Iterator<Cell> iterator(@NotNull final ByteBuffer from) {
        return map.tailMap(from)
                .entrySet()
                .stream()
                .map(element -> new Cell(element.getKey(), element.getValue()))
                .iterator();
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) {
        if (map.containsKey(key)) {
            sizeInBytes += value.remaining() + Long.BYTES;
        } else {
            sizeInBytes += value.remaining() + key.remaining() + Long.BYTES;
        }
        map.put(key.duplicate(), new Value(System.currentTimeMillis(), value.duplicate()));
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) {
        final Value previous = map.put(key.duplicate(), new Value(System.currentTimeMillis()));
        if (previous == null) {
            sizeInBytes += key.remaining();
        } else if (!previous.isTombstone()) {
            sizeInBytes -= previous.getData().remaining();
        }
    }

    @Override
    public void close() {
        map.clear();
    }

    int size() {
        return map.size();
    }

    long sizeInBytes() {
        return sizeInBytes;
    }
}
