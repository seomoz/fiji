/**
 * (c) Copyright 2014 WibiData, Inc.
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

package com.moz.fiji.schema.impl;

import java.io.IOException;
import java.util.Iterator;

import com.google.common.collect.ImmutableList;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.schema.EntityId;
import com.moz.fiji.schema.FijiCell;
import com.moz.fiji.schema.FijiColumnName;
import com.moz.fiji.schema.FijiDataRequest;
import com.moz.fiji.schema.FijiResult;

/**
 * A {@code FijiResult} with no cells.
 *
 * @param <T> The type of {@code FijiCell} values in the view.
 */
@ApiAudience.Private
public class EmptyFijiResult<T> implements FijiResult<T> {

  private final EntityId mEntityId;
  private final FijiDataRequest mDataRequest;

  /**
   * Constructor for an empty fiji result.
   *
   * @param entityId The entity id of the row to which the fiji result belongs.
   * @param dataRequest The data request defining what columns are requested as part of this result.
   */
  public EmptyFijiResult(final EntityId entityId, final FijiDataRequest dataRequest) {
    mEntityId = entityId;
    mDataRequest = dataRequest;
  }

  /** {@inheritDoc} */
  @Override
  public EntityId getEntityId() {
    return mEntityId;
  }

  /** {@inheritDoc} */
  @Override
  public FijiDataRequest getDataRequest() {
    return mDataRequest;
  }

  /** {@inheritDoc} */
  @Override
  public Iterator<FijiCell<T>> iterator() {
    return ImmutableList.<FijiCell<T>>of().iterator();
  }

  /** {@inheritDoc} */
  @Override
  @SuppressWarnings("unchecked")
  public <U extends T> FijiResult<U> narrowView(final FijiColumnName column) {
    final FijiDataRequest narrowRequest = DefaultFijiResult.narrowRequest(column, mDataRequest);
    if (mDataRequest.equals(narrowRequest)) {
      return (FijiResult<U>) this;
    } else {
      return new EmptyFijiResult<U>(mEntityId, narrowRequest);
    }
  }

  /** {@inheritDoc} */
  @Override
  public void close() throws IOException {
  }
}
