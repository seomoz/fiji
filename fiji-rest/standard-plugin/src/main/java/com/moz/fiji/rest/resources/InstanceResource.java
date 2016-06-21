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
import static com.moz.fiji.rest.RoutesConstants.INSTANCE_PATH;

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
import com.moz.fiji.schema.avro.MetadataBackup;

/**
 * This REST resource interacts with Fiji instances.
 *
 * This resource is served for requests using the resource identifier:
 * <ul>
 * <li>/v1/instances/&lt;instance&gt;</li>
 * </ul>
 */
@Path(INSTANCE_PATH)
@Produces(MediaType.APPLICATION_JSON)
@ApiAudience.Public
public class InstanceResource {
  private final FijiClient mFijiClient;

  /**
   * Default constructor.
   *
   * @param fijiClient that this should use for connecting to Fiji.
   */
  public InstanceResource(FijiClient fijiClient) {
    mFijiClient = fijiClient;
  }

  /**
   * GETs a list of instances that are available.
   * @param instance is the instance whose metadata is being requested.
   * @return the metadata about the instance.
   */
  @GET
  @Timed
  @ApiStability.Experimental
  public MetadataBackup getInstanceMetadata(@PathParam(INSTANCE_PARAMETER) String instance) {
    Fiji fiji = mFijiClient.getFiji(instance);
    try {
      MetadataBackup backup = MetadataBackup.newBuilder()
          .setLayoutVersion(fiji.getSystemTable().getDataVersion().toString())
          .setSystemTable(fiji.getSystemTable().toBackup())
          .setSchemaTable(fiji.getSchemaTable().toBackup())
          .setMetaTable(fiji.getMetaTable().toBackup()).build();
      return backup;
    } catch (Exception e) {
      throw new WebApplicationException(e, Status.INTERNAL_SERVER_ERROR);
    }
  }
}
