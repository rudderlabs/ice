/*
 *
 *  Copyright 2013 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */
package com.netflix.ice.processor;

import com.netflix.ice.tag.Operation.SavingsPlanPaymentOption;
import com.netflix.ice.tag.SavingsPlanArn;

public class SavingsPlan {
	final public SavingsPlanArn arn;
	final public SavingsPlanPaymentOption paymentOption;
	final public double hourlyRecurringFee;
	final public double hourlyAmortization;
	final public double normalizedRecurring;
	final public double normalizedAmortization;
	
	public SavingsPlan(String arn, SavingsPlanPaymentOption paymentOption, double hourlyRecurringFee, double hourlyAmortization) {
		this.arn = SavingsPlanArn.get(arn);
		this.paymentOption = paymentOption;
		this.hourlyRecurringFee = hourlyRecurringFee;
		this.hourlyAmortization = hourlyAmortization;
		this.normalizedRecurring = hourlyRecurringFee / (hourlyRecurringFee + hourlyAmortization);
		this.normalizedAmortization = hourlyAmortization / (hourlyRecurringFee + hourlyAmortization);
	}
	
	public double getRecurring(double effectiveCost) {
		return effectiveCost * normalizedRecurring;
	}
	
	public double getAmortization(double effectiveCost) {
		return effectiveCost * normalizedAmortization;
	}
	
	public String toString() {
		return arn.name + "," + paymentOption.name() + "," + ((Double)hourlyRecurringFee).toString() + "," + ((Double)hourlyAmortization).toString();
	}
}
