package com.dtc.java.analytic.V2.common.utils;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.io.ByteArrayOutputStream;

/**
 * @author
 */
public class KyroSerialUtil {

    private static final ThreadLocal<Kryo> KRYOS = ThreadLocal.withInitial(Kryo::new);

    public static final byte[] EMPTY_BYTE_ARRAY = new byte[0];

    public final static byte[] serialize(Object t) {
        if (t == null) {
            return EMPTY_BYTE_ARRAY;
        }
        Kryo kryo = KRYOS.get();
        kryo.setReferences(false);
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream(); Output output = new Output(baos)) {
            kryo.writeClassAndObject(output, t);
            output.flush();
            return baos.toByteArray();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return EMPTY_BYTE_ARRAY;

    }

    public final static Object deserialize(byte[] bytes) {
        if (bytes == null || bytes.length <= 0) {
            return null;
        }
        Kryo kryo = KRYOS.get();
        kryo.setReferences(false);
        try (Input input = new Input(bytes)) {
            return kryo.readClassAndObject(input);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}
