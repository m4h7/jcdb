package com.github.m4h7;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.MappedByteBuffer;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.ArrayList;
import java.util.zip.Inflater;

public class Reader {

    private final List<ByteBuffer> mappings = new ArrayList<ByteBuffer>();
    private final RandomAccessFile cdbFile;
    long MMAP_SIZE = Integer.MAX_VALUE;
    long tableStart = -1;
    long tableLength = 0;

    public Reader(File inputFile) throws FileNotFoundException, IOException {
        cdbFile = new RandomAccessFile(inputFile, "r");
        try {
            long size = cdbFile.length();
            for (long offset = 0; offset < size; offset += MMAP_SIZE) {
                long size2 = Math.min(size - offset, MMAP_SIZE);
                MappedByteBuffer mapping = cdbFile.getChannel().map(FileChannel.MapMode.READ_ONLY, offset, size2);
                mapping.order(ByteOrder.LITTLE_ENDIAN);
                mappings.add(mapping);
            }

            for (int i = 0 ; i < 256 ; ++i) {
                long l = getLong(i * 16);
                if (l < tableStart || tableStart == -1) {
                    tableStart = l;
                }
                long x = getLong(i * 16 + 8);
                tableLength += (x >> 1);
            }
        } catch (IOException e) {
            cdbFile.close();
            throw e;
        }
    }

    //
    // returns the next key in file
    // if prevkey = -1 then return the first entry
    // if return value is equal to -1 then prevkey is the last entry
    //
    public final long nextkey(long prevkey) {
        if (prevkey == -1) {
            long nextkey = 256 * 16;
            if (nextkey <= tableStart) {
                return nextkey;
            } else {
                return -1;
            }
        } else {
            int keyLength = getInt(prevkey);
            int dataLength = getInt(prevkey + 4);
            long nextkey = prevkey + keyLength + dataLength + 4 + 4;
            if (nextkey < tableStart) {
                return nextkey;
            } else {
                return -1;
            }
        }
    }

    public final byte[] keyAt(long keypos) {
        int keyLength = getInt(keypos);
        byte[] keyb = new byte[keyLength];
        getBytes(keyb, keypos + 8, keyLength);
        return keyb;
    }

    public final byte[] valueAt(long keypos) {
        int keyLength = getInt(keypos);
        int valueLength = getInt(keypos + 4);
        byte[] valueb = new byte[valueLength];
        getBytes(valueb, keypos + 8 + keyLength, valueLength);
        try {
            Inflater decompresser = new Inflater();
            decompresser.setInput(valueb, 0, valueLength);
            byte[] result = new byte[valueLength * 5];
            int resultLength = decompresser.inflate(result);
            decompresser.end();

            return result;
        } catch (java.util.zip.DataFormatException e) {
            System.out.println("DataFormatException");
            return valueb;
        }
    }

    public static final int hash(byte[] key) {
        long h = 5381;
        for (int i = 0 ; i < key.length ; ++i) {
            h = (h + (h << 5)) ^ ((key[i] + 0x100) & 0xff);
        }
        return (int)h;
    }

    private void getBytes(byte[] dst, long position, int length) {
        int startN = (int) (position / MMAP_SIZE);
        int offN = (int) (position % MMAP_SIZE);

        int length1 = (int)(MMAP_SIZE - (long)offN);

        ByteBuffer srcBuffer = mappings.get(startN);
        for (int i = 0 ; i < length && i < length1 ; ++i) {
            dst[i] = srcBuffer.get(offN + i);
        }

        int rest = length - length1;

        if (rest > 0) {
            ByteBuffer srcBuffer2 = mappings.get(startN + 1);
            for (int i = 0 ; i < rest ; ++i) {
                dst[length1 + i] = srcBuffer2.get(i);
            }
        }
    }

    private int getInt(long position) {
        int mapN = (int) (position / MMAP_SIZE);
        int offN = (int) (position % MMAP_SIZE);
        return mappings.get(mapN).getInt(offN);
    }

    private long getLong(long position) {
        int mapN = (int) (position / MMAP_SIZE);
        int offN = (int) (position % MMAP_SIZE);
        return mappings.get(mapN).getLong(offN);
    }

    public int count(byte[] key) {
        return (int)internal_get(key, -1);
    }

    // return record position for given key and index
    public long get(byte[] key, int index) {
        return internal_get(key, index);
    }

    private long internal_get(byte[] key, int index) {
        // hash input key
        int h = hash(key);
        // first level lookup in the 256 entries
        int n = h & 0xFF;
        // offset
        long off = getLong(n * 16);
        long nslots = getLong(n * 16 + 8);

        long size = nslots << 4; // * 16
        long end = off + size;

        int hl = h;
        hl >>>= 8;
        long slot_n = hl % nslots;
        long slot_off = off + (slot_n << 4);

        long pos = -1;
        long count = 0;

        long i = slot_off;
        while (true) {
            long rec_h = (int)getLong(i);
            long rec_pos = getLong(i + 8);

            if (rec_pos == 0) {
                break;
            }
            if (rec_h == h) {
                if (index != -1 && count == index) {
                    pos = rec_pos;
                }
                count += 1;
            }
            i += 16;
            // back where we started
            if (i == slot_off) {
                break;
            } else if (i >= end) {
                i = off;
            }
        }

        return (index == -1) ? count : pos;
    }
}
