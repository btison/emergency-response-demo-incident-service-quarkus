package com.redhat.emergency.response.incident.rest;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.redhat.emergency.response.incident.tracing.TracingUtils;
import io.opentracing.Tracer;
import io.smallrye.mutiny.Uni;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.core.eventbus.EventBus;

@Path("/incidents")
public class IncidentsResource {

    @Inject
    EventBus bus;

    @Inject
    Tracer tracer;

    @GET
    @Path("/")
    @Produces(MediaType.APPLICATION_JSON)
    public Uni<Response> incidents() {
        DeliveryOptions options = new DeliveryOptions().addHeader("action", "incidents");
        TracingUtils.inject(options, tracer);
        return bus.<JsonObject>request("incident-service", new JsonObject(), options)
                .onItem().transform(msg -> Response.ok(msg.body().getJsonArray("incidents").encode()).build());
    }

    @POST
    @Path("/")
    @Consumes(MediaType.APPLICATION_JSON)
    public Uni<Response> createIncident(String incident) {
        DeliveryOptions options = new DeliveryOptions().addHeader("action", "createIncident");
        TracingUtils.inject(options, tracer);
        return bus.<JsonObject>request("incident-service", new JsonObject(incident), options)
                .onItem().transform(msg -> Response.status(200).build());
    }

    @GET
    @Path("/{status}")
    @Produces(MediaType.APPLICATION_JSON)
    public Uni<Response> incidentsByStatus(@PathParam("status") String status) {
        DeliveryOptions options = new DeliveryOptions().addHeader("action", "incidentsByStatus");
        TracingUtils.inject(options, tracer);
        return bus.<JsonObject>request("incident-service", new JsonObject().put("status", status), options)
                .onItem().transform(msg -> Response.ok(msg.body().getJsonArray("incidents").encode()).build());
    }

    @GET
    @Path("/incident/{id}")
    @Produces(MediaType.APPLICATION_JSON)
    public Uni<Response> incidentById(@PathParam("id") String incidentId) {
        DeliveryOptions options = new DeliveryOptions().addHeader("action", "incidentById");
        TracingUtils.inject(options, tracer);
        return bus.<JsonObject>request("incident-service", new JsonObject().put("incidentId", incidentId), options)
                .onItem().transform(msg -> {
                    JsonObject incident = msg.body().getJsonObject("incident");
                    if (incident == null) {
                        return Response.status(404).build();
                    } else {
                        return Response.ok(incident.encode()).build();
                    }
                });
    }

    @GET
    @Path("/byname/{name}")
    @Produces(MediaType.APPLICATION_JSON)
    public Uni<Response> incidentsByName(@PathParam("name") String name) {
        DeliveryOptions options = new DeliveryOptions().addHeader("action", "incidentsByName");
        TracingUtils.inject(options, tracer);
        return bus.<JsonObject>request("incident-service", new JsonObject().put("name", name), options)
                .onItem().transform(msg -> Response.ok(msg.body().getJsonArray("incidents").encode()).build());
    }

    @POST
    @Path("/reset")
    public Uni<Response> reset() {
        DeliveryOptions options = new DeliveryOptions().addHeader("action", "reset");
        TracingUtils.inject(options, tracer);
        return bus.<JsonObject>request("incident-service", new JsonObject(), options)
                .onItem().transform(msg -> Response.ok().build());
    }
}
