package ru.mail.polis.sempiternal21;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;

final class Cell implements  Comparable<Cell>{

    @Override
    public int compareTo(@NotNull final Cell cell) {
        final int cmp = key.compareTo(cell.getKey());
        if(cmp == 0){
            return Long.compare(cell.getValue().getTimestamp(), value.getTimestamp());
        }else{
            return cmp;
        }
    }

    @NotNull
    private final ByteBuffer key;
    @NotNull
    private final Value value;

    Cell(@NotNull final ByteBuffer key, @NotNull final Value value) {
        this.key = key;
        this.value = value;
    }

    @NotNull
    public ByteBuffer getKey() {
        return key.asReadOnlyBuffer();
    }

    @NotNull
    public Value getValue() {
        return value;
    }

}
