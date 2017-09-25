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
package com.netflix.ice.tag;

import java.util.Map;

import com.google.common.collect.Maps;

public class Product extends Tag {
	private static final long serialVersionUID = 2L;
	
	/*
	 * Product is a Tag representing AWS products. The name field
	 * stored in the inherited Tag class is used for both the
	 * map key in the ProductService and the product names returned
	 * by the DataManager which are ultimately displayed in the
	 * browser UI.
	 * 
	 * By default the name is the Amazon/AWS name pulled from the billing
	 * reports with the AWS or Amazon prefix removed. For some products
	 * we break down the usage into sub-categories as defined below.
	 * In addition, a few of the product's names are modified to include
	 * the common usage acronym such as EC2 for Elastic Compute Cloud and
	 * S3 for Simple Storage Service. This is helpful when using the
	 * product filter in the browser UI.
	 */
	
	/*
	 * fileName is used for naming product-specific files
	 */
	private final String fileName;
	
	/*
	 * Standard product name strings needed to test identity in the "is" methods.
	 */
	public static final String cloudhsm     = "CloudHSM";
	public static final String dataTransfer = "Data Transfer";
	public static final String ec2          = "Elastic Compute Cloud";
	public static final String rds          = "RDS Service";
	public static final String rdsFull      = "Relational Database Service"; // AWS started using the full name on 2017-09-01
	public static final String redshift     = "Redshift";
	public static final String s3           = "Simple Storage Service";
	public static final String monitor      = "Monitor"; // seems to refer to EC2 metrics, but I've never seen this in any reports -jimroth
    /*
     * ICE-defined product sub-category strings used to test identity in the "is" methods.
     */
    public static final String ebs           = "Elastic Block Storage";
    public static final String ec2CloudWatch = "EC2 CloudWatch";
    public static final String ec2Instance   = "EC2 Instance";
    public static final String eip           = "Elastic IP";
    public static final String rdsInstance   = "RDS Instance";

	/*
	 * Map of product names used to replace the AWS name
	 */
    private static Map<String, String> alternateNames = Maps.newHashMap();
    private static Map<String, String> awsNames = Maps.newHashMap();

    /*
     * Product constructor should only be called by the ProductService.
     * All references to products needs to be through the product service maps.
     * 
     * Product can be constructed using either the AWS name or the alternate name.
     * The inherited tag name value will use the alternate name if present, otherwise
     * it uses the canonical AWS name (AWS and Amazon stripped off the head).
     * 
     * The AWS name is always used by the TagGroup serializer so that changing
     * the alternate name won't corrupt the data files.
     */
    public Product(String name) {  
    	super(getAlternate(canonicalName(name)));
   	
    	// substitute "_" for spaces and make lower case
    	// This operation must be invertible, so we assume product
    	// names don't use underscore!
    	fileName = getAwsName(this.name).replace(" ", "_");
    }

    private static String canonicalName(String name) {
    	String s = name;
    	// Strip off "Amazon" or "AWS"
    	if (s.startsWith("Amazon"))
    		s = s.substring("Amazon".length()).trim();
    	else if (s.startsWith("AWS"))
    		s = s.substring("AWS".length()).trim();
    	return s;
    }
    
    public static String getNameFromFileName(String fileName) {
    	// Invert the operation we did to create the short name
    	return fileName.replace("_", " ");
    }
    
    public static void addAlternate(String awsName, String alternate) {
    	alternateNames.put(awsName,  alternate);
    	awsNames.put(alternate,  awsName);
    }

    protected static String getAlternate(String awsName) {
    	String n;
    	return (n = alternateNames.get(awsName)) != null ? n : awsName;
    }
    
    protected static String getAwsName(String name) {
    	String n;
    	return (n = awsNames.get(name)) != null ? n : name;
    }
    
    public String getAwsName() {
    	String n;
    	return (n = awsNames.get(name)) != null ? n : name;
    }

    public String getFileName() {
    	return fileName;
    }
    
    public boolean isSupport() {
    	return getAwsName().contains("Support");
    }
    
    public boolean isCloudHsm() {
    	return name.equals(getAlternate(cloudhsm));
    }
    
    public boolean isDataTransfer() {
    	return name.equals(getAlternate(dataTransfer));
    }
    
    public boolean isEc2() {
    	return name.equals(getAlternate(ec2));
    }
    
    public boolean isRds() {
    	return name.equals(getAlternate(rds)) || name.equals(getAlternate(rdsFull));
    }
    
    public boolean isRedshift() {
    	return name.equals(getAlternate(redshift));
    }
    
    public boolean isS3() {
    	return name.equals(getAlternate(s3));
    }
    
    public boolean isMonitor() {
    	return name.equals(getAlternate(monitor));
    }
    
    public boolean isEbs() {
    	return name.equals(getAlternate(ebs));
    }
    
    public boolean isEC2CloudWatch() {
    	return name.equals(getAlternate(ec2CloudWatch));
    }
    
    public boolean isEc2Instance() {
    	return name.equals(getAlternate(ec2Instance));
    }
    
    public boolean isEip() {
    	return name.equals(getAlternate(eip));
    }
    
    public boolean isRdsInstance() {
    	return name.equals(getAlternate(rdsInstance));
    }
    
}
