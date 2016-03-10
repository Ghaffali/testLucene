/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.common;

import org.apache.solr.common.ToleratedUpdateError;
import org.apache.solr.common.ToleratedUpdateError.CmdType;

import org.apache.lucene.util.LuceneTestCase;

/** Basic testing of the serialization/encapsulation code in ToleratedUpdateError */
public class TestToleratedUpdateError extends LuceneTestCase {
  
  // nocommit: add randomized testing, particularly with non-trivial 'id' values

  public void checkRoundTripComparisons(Coppier coppier) {

    assertNull(ToleratedUpdateError.parseMetadataIfToleratedUpdateError("some other key", "some value"));
    
    for (ToleratedUpdateError in : new ToleratedUpdateError[] {
        new ToleratedUpdateError(CmdType.ADD, "doc1", "some error"),
        new ToleratedUpdateError(CmdType.DELID, "doc1", "some diff error"),
        new ToleratedUpdateError(CmdType.DELQ, "-field:yakko other_field:wakko", "some other error"),
      }) {
      
      ToleratedUpdateError out = coppier.copy(in);
      
      assertNotNull(out);
      assertEquals(out.type, in.type);
      assertEquals(out.id, in.id);
      assertEquals(out.errorValue, in.errorValue);
      assertEquals(out.hashCode(), in.hashCode());
      assertEquals(out.toString(), in.toString());

      assertEquals(in.getMetadataKey(), out.getMetadataKey());
      assertEquals(in.getMetadataValue(), out.getMetadataValue());
      
      assertEquals(out, in);
      assertEquals(in, out);

    }
    
    assertFalse((new ToleratedUpdateError(CmdType.ADD, "doc1", "some error")).equals
                (new ToleratedUpdateError(CmdType.ADD, "doc2", "some error")));
    assertFalse((new ToleratedUpdateError(CmdType.ADD, "doc1", "some error")).equals
                (new ToleratedUpdateError(CmdType.ADD, "doc1", "some errorxx")));
    assertFalse((new ToleratedUpdateError(CmdType.ADD, "doc1", "some error")).equals
                (new ToleratedUpdateError(CmdType.DELID, "doc1", "some error")));
    
  }
  
  public void testMetadataRoundTripComparisons(Coppier coppier) {
    checkRoundTripComparisons(new Coppier() {
      public ToleratedUpdateError copy(ToleratedUpdateError in) {
        return ToleratedUpdateError.parseMetadataIfToleratedUpdateError
          (in.getMetadataKey(), in.getMetadataValue());
      }
    });
  }
  
  public void testMapRoundTripComparisons() {
    checkRoundTripComparisons(new Coppier() {
      public ToleratedUpdateError copy(ToleratedUpdateError in) {
        return ToleratedUpdateError.parseMap(in.getSimpleMap());
      }
    });
  }

  private static abstract class Coppier {
    public abstract ToleratedUpdateError copy(ToleratedUpdateError in);
  }
}




