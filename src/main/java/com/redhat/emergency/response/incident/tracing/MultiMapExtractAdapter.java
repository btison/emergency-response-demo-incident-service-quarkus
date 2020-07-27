package com.redhat.emergency.response.incident.tracing;

import java.util.Iterator;
import java.util.Map;

import io.opentracing.propagation.TextMap;
import io.vertx.core.MultiMap;

public class MultiMapExtractAdapter implements TextMap {

    private final MultiMap map;

    public MultiMapExtractAdapter(MultiMap headers) {
        this.map = headers;
    }

    public MultiMapExtractAdapter(io.vertx.mutiny.core.MultiMap headers) {
        this.map = headers.getDelegate();
    }

    @Override
    public Iterator<Map.Entry<String, String>> iterator() {
        return map.iterator();
    }

    @Override
    public void put(String key, String value) {
        throw new UnsupportedOperationException("ExtractAdapter should only be used with Tracer.extract()");
    }
}
