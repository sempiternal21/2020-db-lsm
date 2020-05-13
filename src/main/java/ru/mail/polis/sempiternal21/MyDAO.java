package ru.mail.polis.sempiternal21;

import com.google.common.collect.Iterators;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.DAO;
import ru.mail.polis.Iters;
import ru.mail.polis.Record;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.logging.Logger;

public class MyDAO implements DAO {
    private static final String SUFFIX = ".dat";
    private static final String TEMP = ".tmp";
    private static Logger log = Logger.getLogger(MyDAO.class.getName());

    private final File storage;
    private final long flushThreshold;

    //Data
    private MemTable memTable;
    private final NavigableMap<Integer, Table> ssTables;

    //State
    private int version;

    /**
     * Realization of LSMDAO.
     *
     * @param storage        - SSTable storage directory
     * @param flushThreshold - max size of MemTable
     */
    public MyDAO(@NotNull final File storage, final long flushThreshold) throws IOException {
        assert flushThreshold > 0L;
        this.flushThreshold = flushThreshold;
        this.storage = storage;
        this.ssTables = new TreeMap<>();
        this.memTable = new MemTable();
        version = -1;
        final File[] list = storage.listFiles((dir1, name) -> name.endsWith(SUFFIX));
        assert list != null;
        Arrays.stream(list)
                .filter(currentFile -> !currentFile.isDirectory())
                .forEach(f -> {
                            final String name = f.getName();
                            final String sub = name.substring(0, name.indexOf(SUFFIX));
                            if (sub.matches("[0-9]+")) {
                                final int gen =
                                        Integer.parseInt(sub);
                                try {
                                    ssTables.put(gen, new SSTable(f));
                                } catch (IOException e) {
                                    log.info("String 63");
                                }
                                if (gen > version) {
                                    version = gen;
                                }
                            }
                        }
                );
        version++;
    }

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull final ByteBuffer from) throws IOException {
        final List<Iterator<Cell>> iterators = new ArrayList<>(ssTables.size() + 1);
        iterators.add(memTable.iterator(from));
        ssTables.descendingMap().values().forEach(t -> {
            try {
                iterators.add(t.iterator(from));
            } catch (IOException e) {
                log.info("String 83");
            }
        });
        final Iterator<Cell> mergedIterator = Iterators.mergeSorted(iterators, Cell.COMPARATOR);
        final Iterator<Cell> fresh = Iters.collapseEquals(mergedIterator, Cell::getKey);
        final Iterator<Cell> alive = Iterators.filter(fresh, e -> !e.getValue().isTombstone());
        return Iterators.transform(alive, e -> Record.of(e.getKey(), e.getValue().getData()));
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) throws IOException {
        memTable.upsert(key, value);
        if (memTable.sizeInBytes() > flushThreshold) {
            flush();
        }
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) throws IOException {
        memTable.remove(key);
        if (memTable.sizeInBytes() > flushThreshold) {
            flush();
        }
    }

    private void flush() throws IOException {
        //Dump memTable
        final File file = new File(storage, version + TEMP);
        SSTable.serialize(file, memTable.iterator(ByteBuffer.allocate(0)));
        final File dst = new File(storage, version + SUFFIX);
        Files.move(file.toPath(), dst.toPath(), StandardCopyOption.ATOMIC_MOVE);

        //Switch
        memTable = new MemTable();
        ssTables.put(version, new SSTable(dst));
        version++;
    }

    @Override
    public void close() throws IOException {
        if (memTable.size() > 0) {
            flush();
        }
        for (int i = 0; i < ssTables.size(); i++) {
            ssTables.get(i).close();
        }
    }
}
