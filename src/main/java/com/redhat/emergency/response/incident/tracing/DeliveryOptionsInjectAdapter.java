package com.redhat.emergency.response.incident.tracing;

import java.util.Iterator;
import java.util.Map;

import io.opentracing.propagation.TextMap;
import io.vertx.core.eventbus.DeliveryOptions;

public class DeliveryOptionsInjectAdapter implements TextMap {

    private final DeliveryOptions deliveryOptions;

    DeliveryOptionsInjectAdapter(DeliveryOptions deliveryOptions) {
        this.deliveryOptions = deliveryOptions;
    }

    @Override
    public Iterator<Map.Entry<String, String>> iterator() {
        throw new UnsupportedOperationException("iterator should never be used with Tracer.inject()");
    }

    @Override
    public void put(String key, String value) {
        deliveryOptions.addHeader(key, value);
    }
}
