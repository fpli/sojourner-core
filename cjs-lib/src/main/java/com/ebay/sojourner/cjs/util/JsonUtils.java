package com.ebay.sojourner.cjs.util;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import lombok.experimental.UtilityClass;
import lombok.val;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

@UtilityClass
public class JsonUtils {

    public ObjectMapper objectMapper() {
        val objectMapper = new ObjectMapper();
        objectMapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        objectMapper.configure(DeserializationFeature.FAIL_ON_IGNORED_PROPERTIES, false);
        return objectMapper;
    }

    public <K, V> Map<K, V> loadJsonFileFromStream(InputStream inputStream, Function<V, K> keyGetter,
                                                   TypeReference<List<V>> typeRef) throws IOException {
        if (Objects.isNull(inputStream)) {
            throw new IOException("inputStream is null");
        }
        return objectMapper().readValue(inputStream, typeRef).stream()
                             .collect(Collectors.toMap(keyGetter, Function.identity()));
    }

}
