/**
 * (c) Copyright 2013 WibiData, Inc.
 *
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.moz.fiji.rest.resources;

import static com.moz.fiji.rest.RoutesConstants.INSTANCE_PARAMETER;
import static com.moz.fiji.rest.RoutesConstants.TABLE_PARAMETER;
import static com.moz.fiji.rest.RoutesConstants.TABLE_PATH;

import java.io.IOException;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response.Status;

import com.codahale.metrics.annotation.Timed;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.annotations.ApiStability;
import com.moz.fiji.rest.FijiClient;
import com.moz.fiji.schema.Fiji;
import com.moz.fiji.schema.avro.TableLayoutDesc;

/**
 * This REST resource interacts with Fiji tables.
 *
 * This resource is served for requests using the resource identifier:
 * <li>/v1/instances/&lt;instance&gt;/tables/&lt;table&gt;
 */
@Path(TABLE_PATH)
@Produces(MediaType.APPLICATION_JSON)
@ApiAudience.Public
public class TableResource {
  private final FijiClient mFijiClient;

  /**
   * Default constructor.
   *
   * @param fijiClient that this should use for connecting to Fiji.
   */
  public TableResource(FijiClient fijiClient) {
    mFijiClient = fijiClient;
  }

  /**
   * GETs the layout of the specified table.
   *
   * @param instance in which the table resides.
   * @param table to get the layout for.
   * @return the layout of the specified table
   */
  @GET
  @Timed
  @ApiStability.Evolving
  public TableLayoutDesc getTable(@PathParam(INSTANCE_PARAMETER) String instance,
      @PathParam(TABLE_PARAMETER) String table) {
    final Fiji fiji = mFijiClient.getFiji(instance);
    TableLayoutDesc layout = null;
    try {
      layout = fiji.getMetaTable().getTableLayout(table).getDesc();
    } catch (IOException e) {
      throw new WebApplicationException(e, Status.INTERNAL_SERVER_ERROR);
    }
    return layout;
  }
}
