/**
 * (c) Copyright 2015 WibiData, Inc.
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

package com.moz.fiji.hadoop.configurator;

/**
 * An exception thrown when there is an error populating the member
 * variables of a Configurable instance.
 */
public class HadoopConfigurationException extends RuntimeException {
  /**
   * Constructs the exception.
   *
   * @param message A message.
   */
  public HadoopConfigurationException(String message) {
    super(message);
  }

  /**
   * Constructs the exception.
   *
   * @param cause The cause.
   */
  public HadoopConfigurationException(Throwable cause) {
    super(cause);
  }
}
