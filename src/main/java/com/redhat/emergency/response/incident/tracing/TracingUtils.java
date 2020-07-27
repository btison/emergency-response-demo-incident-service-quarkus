package com.redhat.emergency.response.incident.tracing;

import io.opentracing.Span;
import io.opentracing.SpanContext;
import io.opentracing.Tracer;
import io.opentracing.propagation.Format;
import io.opentracing.tag.Tags;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.core.eventbus.Message;

public class TracingUtils {

    public static void inject(DeliveryOptions options, Tracer tracer) {
        Span span =  tracer.activeSpan();
        if (span != null) {
            tracer.inject(span.context(), Format.Builtin.TEXT_MAP, new DeliveryOptionsInjectAdapter(options));
        }
    }

    public static Span activateSpan(String operation, Message<JsonObject> message, Tracer tracer) {
        SpanContext spanContext = tracer.extract(Format.Builtin.TEXT_MAP, new MultiMapExtractAdapter(message.headers()));

        Tracer.SpanBuilder spanBuilder = tracer.buildSpan(operation).ignoreActiveSpan()
                .withTag(Tags.SPAN_KIND.getKey(), "eventBus");
        if (spanContext != null) {
            spanBuilder.asChildOf(spanContext);
        }

        return spanBuilder.startActive(true).span();

    }

}
