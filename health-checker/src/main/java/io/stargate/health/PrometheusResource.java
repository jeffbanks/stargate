/*
 * Copyright The Stargate Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.stargate.health;

import io.micrometer.prometheus.PrometheusMeterRegistry;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * Simple Prometheus resource that exposes any {@link PrometheusMeterRegistry} to the <code>
 * /metrics</code> endpoint.
 */
@Path("/metrics")
public class PrometheusResource {

  @Inject private PrometheusMeterRegistry registry;

  @GET
  @Produces(MediaType.TEXT_PLAIN)
  public Response prometheusEndpoint() {
    return Response.ok(registry.scrape()).build();
  }
}
