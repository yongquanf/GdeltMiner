/*******************************************************************************
 * Copyright (c) 2010 Haifeng Li
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
 *******************************************************************************/

package util.Util.smile.stat.distribution;

/**
 * This is the base class of multivariate distributions. Likelihood and
 * log likelihood functions are implemented here.
 *
 * @author Haifeng Li
 */
public abstract class AbstractMultivariateDistribution implements MultivariateDistribution {

    /**
     * The likelihood given a sample set following the distribution.
     */
    @Override
    public double likelihood(double[][] x) {
        return Math.exp(logLikelihood(x));
    }

    /**
     * The likelihood given a sample set following the distribution.
     */
    @Override
    public double logLikelihood(double[][] x) {
        double L = 0.0;

        for (double[] xi : x)
            L += logp(xi);

        return L;
    }
}
