package org.apache.flink.table.functions.utils;

import com.google.common.base.Preconditions;
import org.apache.hadoop.io.Writable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class WritableSerializer {

    /**
     * Serialize writable byte[].
     *
     * @param <T>      the type parameter
     * @param writable the writable
     * @return the byte [ ]
     * @throws IOException the io exception
     */
    public static <T extends Writable> byte[] serializeWritable(T writable) throws IOException {
        Preconditions.checkArgument(writable != null);

        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream);
        writable.write(outputStream);
        return byteArrayOutputStream.toByteArray();
    }

    /**
     * Deserialize writable.
     *
     * @param <T>      the type parameter
     * @param writable the writable
     * @param bytes    the bytes
     * @throws IOException the io exception
     */
    public static <T extends Writable> void deserializeWritable(T writable, byte[] bytes)
            throws IOException {
        Preconditions.checkArgument(writable != null);
        Preconditions.checkArgument(bytes != null);

        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
        DataInputStream dataInputStream = new DataInputStream(byteArrayInputStream);
        writable.readFields(dataInputStream);
    }

}
