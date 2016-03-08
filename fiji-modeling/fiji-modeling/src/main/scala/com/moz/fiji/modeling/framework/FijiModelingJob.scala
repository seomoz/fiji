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

package com.moz.fiji.modeling.framework

import com.twitter.scalding.Args

import com.moz.fiji.express.flow.FijiJob
import com.moz.fiji.annotations.ApiAudience
import com.moz.fiji.annotations.ApiStability
import com.moz.fiji.annotations.Inheritance

/**
 * FijiModelJob is an extension of FijiJob that includes implicits for modeling.
 *
 * Users should extend this class when writing jobs that use modeling.
 *
 * @param args to the job.
 */
@ApiAudience.Public
@ApiStability.Evolving
@Inheritance.Extensible
class FijiModelingJob(args: Args) extends FijiJob(args) with ModelPipeConversions
