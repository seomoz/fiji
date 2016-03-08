/**
 * (c) Copyright 2012 WibiData, Inc.
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

package com.moz.fiji.schema.tools.synth;

import static org.easymock.EasyMock.createStrictMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class TestNGramSynthesizer {

  @Test
  public void testWordSynth() {
    WordSynthesizer wordSynthesizer = createStrictMock(WordSynthesizer.class);
    expect(wordSynthesizer.synthesize()).andReturn("a");
    expect(wordSynthesizer.synthesize()).andReturn("b");
    expect(wordSynthesizer.synthesize()).andReturn("c");
    expect(wordSynthesizer.synthesize()).andReturn("d");

    replay(wordSynthesizer);
    NGramSynthesizer synth = new NGramSynthesizer(wordSynthesizer, 2);
    assertEquals("a b", synth.synthesize());
    assertEquals("c d", synth.synthesize());
    verify(wordSynthesizer);
  }
}
