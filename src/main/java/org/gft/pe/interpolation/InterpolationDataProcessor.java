/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.gft.pe.interpolation;

import org.apache.commons.math3.analysis.interpolation.LoessInterpolator;
import org.apache.streampipes.commons.exceptions.SpRuntimeException;
import org.apache.streampipes.model.DataProcessorType;
import org.apache.streampipes.model.graph.DataProcessorDescription;
import org.apache.streampipes.model.runtime.Event;
import org.apache.streampipes.model.schema.PropertyScope;
import org.apache.streampipes.sdk.builder.PrimitivePropertyBuilder;
import org.apache.streampipes.sdk.builder.ProcessingElementBuilder;
import org.apache.streampipes.sdk.builder.StreamRequirementsBuilder;
import org.apache.streampipes.sdk.helpers.*;
import org.apache.streampipes.sdk.utils.Assets;
import org.apache.streampipes.sdk.utils.Datatypes;
import org.apache.streampipes.wrapper.context.EventProcessorRuntimeContext;
import org.apache.streampipes.wrapper.routing.SpOutputCollector;
import org.apache.streampipes.wrapper.standalone.ProcessorParams;
import org.apache.streampipes.wrapper.standalone.StreamPipesDataProcessor;

import org.apache.commons.math3.analysis.interpolation.LinearInterpolator;
import org.apache.commons.math3.analysis.polynomials.PolynomialSplineFunction;

import java.math.BigDecimal;
import java.math.RoundingMode;

public class InterpolationDataProcessor extends StreamPipesDataProcessor {

  private String input_value;
  private String timestamp_value;
  private String interpolation_operation;

  private static final String INPUT_VALUE = "value";
  private static final String TIMESTAMP_VALUE = "timestamp_value";
  private static final String THRESHOLD = "threshold";
  private static final String INTERPOLATION_OPERATION = "interpolation_operation";

  private Double threshold;
  double[] arrayX = {0.0,0.0};
  double[] arrayY = {0.0,0.0};


  @Override
  public DataProcessorDescription declareModel() {
    return ProcessingElementBuilder.create("org.gft.pe.interpolation.processor")
            .withAssets(Assets.DOCUMENTATION, Assets.ICON)
            .withLocales(Locales.EN)
            .category(DataProcessorType.AGGREGATE)

            .requiredStream(StreamRequirementsBuilder.create()
                    .requiredPropertyWithUnaryMapping(EpRequirements.numberReq(),
                            Labels.withId(INPUT_VALUE), PropertyScope.NONE)
                    .requiredPropertyWithUnaryMapping(EpRequirements.numberReq(),
                            Labels.withId(TIMESTAMP_VALUE), PropertyScope.NONE)
                    .build())
            .requiredSingleValueSelection(Labels.withId(INTERPOLATION_OPERATION), Options.from("Linear", "Loess"))

            .requiredFloatParameter(Labels.withId(THRESHOLD))

            .outputStrategy(OutputStrategies.append(PrimitivePropertyBuilder.create(Datatypes.Double, "chosen_timestamp").build()
                    ,PrimitivePropertyBuilder.create(Datatypes.Double, "interpolation_value").build()))

            .build();
  }

  @Override
  public void onInvocation(ProcessorParams processorParams,
                            SpOutputCollector out,
                            EventProcessorRuntimeContext ctx) throws SpRuntimeException  {

    this.input_value = processorParams.extractor().mappingPropertyValue(INPUT_VALUE);
    this.timestamp_value = processorParams.extractor().mappingPropertyValue(TIMESTAMP_VALUE);
    this.threshold = processorParams.extractor().singleValueParameter(THRESHOLD,Double.class);

  }

  @Override
  public void onEvent(Event event,SpOutputCollector out){

    Double xi;
    Double yi;

    //recovery input value
    Double value = event.getFieldBySelector(this.input_value).getAsPrimitive().getAsDouble();

    //recovery timestamp value
    String timestampStr = event.getFieldBySelector(this.timestamp_value).getAsPrimitive().getAsString();

    //convert timestamp to double
    Double timestamp= Double.parseDouble(timestampStr);

    //recover type of interpolation
    String interpolation=event.getFieldBySelector(this.interpolation_operation).getAsPrimitive().getAsString();

    //if we are in the first event it sets the [0] values of the two arrays with the data arriving from SP
    if((arrayY[0]==0.0 && arrayX[0]==0.0)) {

      //System.out.println("DENTRO IF" );
      arrayX[0] = timestamp;
      arrayY[0] = value;

    //if the new timestamp is equal than the timestamp previously or the difference is more low to the threshold,
    //do not perform an interpolation
    }else if((arrayX[0]==timestamp) || (timestamp-arrayX[0]< this.threshold)){
      System.out.println("--------- Timestamp Values not accepted ------- " );

    //perform an interpolation
    }else {
      arrayX[1]=timestamp;
      arrayY[1]=value;

      //perform a mathematical median for an array of two timestamp values
      BigDecimal bd = new BigDecimal((arrayX[0]+arrayX[1])/2).setScale(2, RoundingMode.HALF_UP);
      xi = bd.doubleValue();

      if (interpolation == "Linear"){
        //perform a linear interpolation
        yi = linearInterp(arrayX,arrayY, xi);
      } else {// if(interpolation == "Loess"){
        //perform a Loess interpolation
        yi = loessInterp(arrayX,arrayY, xi);
      }

      //move the second value of the array to the first position
      arrayX[0]=arrayX[1];
      arrayY[0]=arrayY[1];

      //set the values resulting from the interpolation, in the fields of the event output
      event.addField("appendedText_xi", xi);
      event.addField("appendedText_yi", yi);

      out.collect(event);


    }

  }

  @Override
  public void onDetach(){
  }


  public double linearInterp(double[] x, double[] y, double xi) {
    // return linear interpolation of (x,y) on xi
    LinearInterpolator li = new LinearInterpolator();
    PolynomialSplineFunction psf = li.interpolate(x, y);
    double yi = psf.value(xi);
    return yi;
  }

  public double loessInterp(double[] x, double[] y, double xi) {
    LoessInterpolator li = new LoessInterpolator();
    PolynomialSplineFunction psf = li.interpolate(x,y);
    double yi = psf.value(xi);
    return yi;
  }



}
