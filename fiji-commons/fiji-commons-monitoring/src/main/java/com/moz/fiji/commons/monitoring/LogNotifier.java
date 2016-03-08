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

package com.moz.fiji.commons.monitoring;

import java.util.Map;

import com.google.common.base.Joiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A notifier which logs all notifications to an SLF4J logger.
 */
public final class LogNotifier implements Notifier {
  private final Logger mLog;

  /**
   * Construct a new log notifier.
   *
   * @param logger The logger name to use when logging.
   */
  private LogNotifier(final String logger) {
    mLog = LoggerFactory.getLogger(logger);
  }

  /**
   * Create a new log notifier.
   *
   * @param logger The logger name to use when logging.
   * @return A new logging notifier.
   */
  public static LogNotifier create(final String logger) {
    return new LogNotifier(logger);
  }

  /** {@inheritDoc} */
  @Override
  public void error(
      final String action,
      final Map<String, String> attributes,
      final Throwable error
  ) {
    final StringBuilder sb = new StringBuilder();
    sb.append("Error in ");
    sb.append(action);
    sb.append(":\n");
    Joiner.on('\n').withKeyValueSeparator(": ").appendTo(sb, attributes);
    mLog.error(sb.toString(), error);
  }

  /** {@inheritDoc} */
  @Override
  public void error(
      final String action,
      final Map<String, String> attributes
  ) {
    final StringBuilder sb = new StringBuilder();
    sb.append("Error in ");
    sb.append(action);
    sb.append(":\n");
    Joiner.on('\n').withKeyValueSeparator(": ").appendTo(sb, attributes);
    mLog.error(sb.toString());
  }

  /** {@inheritDoc} */
  @Override
  public void close() {
    // do nothing
  }
}
