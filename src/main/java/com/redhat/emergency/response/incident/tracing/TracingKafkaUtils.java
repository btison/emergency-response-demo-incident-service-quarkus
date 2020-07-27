package com.redhat.emergency.response.incident.tracing;

import io.opentracing.References;
import io.opentracing.Span;
import io.opentracing.SpanContext;
import io.opentracing.Tracer;
import io.opentracing.tag.Tags;
import io.smallrye.reactive.messaging.kafka.IncomingKafkaRecord;
import io.smallrye.reactive.messaging.kafka.KafkaRecord;

public class TracingKafkaUtils {

    public static void buildAndInjectSpan(KafkaRecord<String, String> record, Tracer tracer) {
        Tracer.SpanBuilder spanBuilder = tracer.buildSpan("To_" + record.getTopic()).withTag(Tags.SPAN_KIND.getKey(), Tags.SPAN_KIND_PRODUCER)
                .withTag("key", record.getKey());

        Span active = tracer.activeSpan();
        if (active != null) {
            spanBuilder.asChildOf(active);
        }
        Span span = spanBuilder.start();
        span.finish();
        io.opentracing.contrib.kafka.TracingKafkaUtils.inject(span.context(),record.getHeaders(), tracer );
    }

    public static Span buildChildSpan(String operationName, IncomingKafkaRecord<String, String> record, Tracer tracer) {

        SpanContext parentContext = io.opentracing.contrib.kafka.TracingKafkaUtils.extractSpanContext(record.getHeaders(), tracer);

        String consumerOperation = "FROM_" + record.getTopic();
        Tracer.SpanBuilder spanBuilder = tracer
                .buildSpan(consumerOperation)
                .ignoreActiveSpan()
                .withTag(Tags.SPAN_KIND.getKey(), Tags.SPAN_KIND_CONSUMER);

        if (parentContext != null) {
            spanBuilder.addReference(References.FOLLOWS_FROM, parentContext);
        }

        Span span = spanBuilder.start();
        span.setTag("partition", record.getPartition())
                .setTag("topic", record.getTopic())
                .setTag("offset", record.getOffset())
                .setTag("key", record.getKey())
                .setTag(Tags.PEER_SERVICE.getKey(), "kafka");
        span.finish();

        //Create new child span
        return tracer.buildSpan(operationName).ignoreActiveSpan()
                .withTag(Tags.SPAN_KIND.getKey(), Tags.SPAN_KIND_CONSUMER)
                .asChildOf(span).startActive(true).span();
    }

}
