package ru.mail.polis.sempiternal21;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

public interface Table {

    @NotNull
    Iterator<Cell> iterator(@NotNull ByteBuffer from) throws IOException;


    /**
     * Inserts or updates value by given key.
     */
    void upsert(
            @NotNull ByteBuffer key,
            @NotNull ByteBuffer value) throws IOException;

    /**
     * Removes value by given key.
     */
    void remove(@NotNull ByteBuffer key) throws IOException;

    void close() throws IOException;
}
