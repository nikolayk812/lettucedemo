package com.lettuce.demo;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.lambdaworks.redis.codec.RedisCodec;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

@Component
public class SomeValueCodec implements RedisCodec<String, SomeValue> {
    private static final Charset UTF8_CHARSET = Charset.forName("UTF-8");
    private static final ObjectMapper objectMapper = new ObjectMapper();

    static {
        // SERIALIZATION
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_DEFAULT);
        objectMapper.configure(SerializationFeature.WRITE_DATE_TIMESTAMPS_AS_NANOSECONDS, false);

        // DESERIALIZATION
        objectMapper.configure(DeserializationFeature.READ_DATE_TIMESTAMPS_AS_NANOSECONDS, false);
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        objectMapper.configure(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_AS_NULL, true);

        objectMapper.enable(JsonParser.Feature.ALLOW_NON_NUMERIC_NUMBERS);
    }

    @Override
    public String decodeKey(ByteBuffer bytes) {
        return new String(bytes.array(), UTF8_CHARSET);
    }

    @Override
    public SomeValue decodeValue(ByteBuffer buffer) {
        byte[] array = new byte[buffer.remaining()];
        buffer.get(array);

        try {
            return objectMapper.readValue(array, SomeValue.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public ByteBuffer encodeKey(String key) {
        return ByteBuffer.wrap(key.getBytes(UTF8_CHARSET));
    }

    @Override
    public ByteBuffer encodeValue(SomeValue value) {
        byte[] bytes;
        try {
            bytes = objectMapper.writeValueAsBytes(value);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        return ByteBuffer.wrap(bytes);
    }
}
