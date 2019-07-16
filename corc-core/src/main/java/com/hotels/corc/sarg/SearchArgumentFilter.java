/**
 * Copyright (C) 2015-2019 Expedia Inc.
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
package com.hotels.corc.sarg;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument.TruthValue;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;

import com.hotels.corc.Corc;
import com.hotels.corc.Filter;

public class SearchArgumentFilter implements Filter {

  private final SearchArgument searchArgument;
  private final List<Evaluator<?>> evaluators;

  public SearchArgumentFilter(SearchArgument searchArgument, StructTypeInfo structTypeInfo) {
    this.searchArgument = searchArgument;
    EvaluatorFactory evaluatorFactory = new EvaluatorFactory(structTypeInfo);
    evaluators = new ArrayList<>(searchArgument.getLeaves().size());
    for (PredicateLeaf predicateLeaf : searchArgument.getLeaves()) {
      evaluators.add(evaluatorFactory.newInstance(predicateLeaf));
    }
  }

  @Override
  public boolean accept(Corc corc) throws IOException {
    TruthValue[] truthValues = new TruthValue[evaluators.size()];
    for (int i = 0; i < evaluators.size(); i++) {
      truthValues[i] = evaluators.get(i).evaluate(corc);
    }
    TruthValue truthValue = searchArgument.evaluate(truthValues);
    switch (truthValue) {
    case YES:
      return true;
    default:
      return false;
    }
  }

}
