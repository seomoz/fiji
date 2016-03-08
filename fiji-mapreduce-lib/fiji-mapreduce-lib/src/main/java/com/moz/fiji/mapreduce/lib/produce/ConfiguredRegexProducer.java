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

package com.moz.fiji.mapreduce.lib.produce;

import org.apache.hadoop.conf.Configuration;

import com.moz.fiji.hadoop.configurator.HadoopConf;
import com.moz.fiji.hadoop.configurator.HadoopConfigurator;

/**
 * A regex producer that is ready to use "out of the box."  All you
 * need to do is specify a couple of configuration values.
 *
 * Here is an example of the EmailDomainProfiler implemented with
 * ConfiguredRegexProducer that reads from "info:email" and writes to
 * "derived:domain":
 * <pre>
 * bin/fiji produce \
 *   -Dfiji.regexproducer.input.column=info:email \
 *   -Dfiji.regexproducer.output.column=derived:domain \
 *   -Dfiji.regexproducer.regex='[^@]+@(.*)' \
 *   --input="format=fiji table=fiji://.env/default/foo" \
 *   --output="format=fiji table=fiji://.env/default/foo nsplits=2" \
 *   --producer=com.moz.fiji.mapreduce.lib.produce.ConfiguredRegexProducer
 * </pre>
 */
public class ConfiguredRegexProducer extends RegexProducer {
  public static final String CONF_INPUT_COLUMN = "fiji.regexproducer.input.column";
  public static final String CONF_OUTPUT_COLUMN = "fiji.regexproducer.output.column";
  public static final String CONF_REGEX = "fiji.regexproducer.regex";

  @HadoopConf(key=CONF_INPUT_COLUMN, usage="The input column name.")
  private String mInputColumn;

  @HadoopConf(key=CONF_OUTPUT_COLUMN, usage="The output column name.")
  private String mOutputColumn;

  @HadoopConf(key=CONF_REGEX, usage="The regular expression used to extract from the input column.")
  private String mRegex;

  @Override
  public void setConf(Configuration conf) {
    super.setConf(conf);
    HadoopConfigurator.configure(this);
  }

  @Override
  protected String getInputColumn() {
    if (null == mInputColumn || mInputColumn.isEmpty()) {
      throw new RuntimeException(CONF_INPUT_COLUMN + " not found");
    }
    return mInputColumn;
  }

  @Override
  protected String getRegex() {
    if (null == mRegex || mRegex.isEmpty()) {
      throw new RuntimeException(CONF_REGEX + " not found");
    }
    return mRegex;
  }

  @Override
  public String getOutputColumn() {
    if (null == mOutputColumn || mOutputColumn.isEmpty()) {
      throw new RuntimeException(CONF_OUTPUT_COLUMN + " not found");
    }
    int colon = mOutputColumn.indexOf(':');
    if (colon < 0) {
      throw new RuntimeException(CONF_OUTPUT_COLUMN + " must be in family:qualifier format");
    }
    return mOutputColumn;
  }
}
