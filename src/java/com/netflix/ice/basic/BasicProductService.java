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
package com.netflix.ice.basic;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.ice.common.ProductService;
import com.netflix.ice.tag.Product;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentMap;

public class BasicProductService implements ProductService {

    private static ConcurrentMap<String, Product> productsByAwsName = Maps.newConcurrentMap();
    private static ConcurrentMap<String, Product> productsByName = Maps.newConcurrentMap();

    public static final BasicProduct api_gateway = new BasicProduct(Product.api_gateway, "API Gateway");
    public static final BasicProduct athena = new BasicProduct(Product.athena, "Athena");
    public static final BasicProduct cloudfront = new BasicProduct(Product.cloudfront, "CloudFront");
    public static final BasicProduct cloudhsm = new BasicProduct(Product.cloudhsm, "CloudHSM");
    public static final BasicProduct cloudtrail = new BasicProduct(Product.cloudtrail, "CloudTrail");
    public static final BasicProduct cloudwatch = new BasicProduct(Product.cloudwatch, "AmazonCloudWatch");
    public static final BasicProduct codecommit = new BasicProduct(Product.codecommit, "CodeCommit");
    public static final BasicProduct config = new BasicProduct(Product.config, "Config");
    public static final BasicProduct data_pipeline = new BasicProduct(Product.data_pipeline, "Data Pipeline");
    public static final BasicProduct data_transfer = new BasicProduct(Product.data_transfer, "Data Transfer");
    public static final BasicProduct direct_connect = new BasicProduct(Product.direct_connect, "Direct Connect");
    public static final BasicProduct database_migration = new BasicProduct(Product.database_migration, "Database Migration Service");
    public static final BasicProduct directory = new BasicProduct(Product.directory, "Directory Service");
    public static final BasicProduct dynamodb = new BasicProduct(Product.dynamodb, "DynamoDB");
    public static final BasicProduct ebs = new BasicProduct(Product.ebs, "ebs");
    public static final BasicProduct ec2 = new BasicProduct(Product.ec2, "Elastic Compute Cloud");
    public static final BasicProduct ec2_cloudwatch = new BasicProduct(Product.ec2_cloudwatch, "CloudWatch");
    public static final BasicProduct ec2_instance = new BasicProduct(Product.ec2_instance, "ec2_instance");
    public static final BasicProduct ecr = new BasicProduct(Product.ecr, "EC2 Container Registry (ECR)");
    public static final BasicProduct efs = new BasicProduct(Product.efs, "Elastic File System");
    public static final BasicProduct eip = new BasicProduct(Product.eip, "eip");
    public static final BasicProduct elasticache = new BasicProduct(Product.elasticache, "ElastiCache");
    public static final BasicProduct elasticsearch = new BasicProduct(Product.elasticsearch, "Elasticsearch Service");
    public static final BasicProduct emr = new BasicProduct(Product.emr, "Elastic MapReduce");
    public static final BasicProduct glacier = new BasicProduct(Product.glacier, "Glacier");
    public static final BasicProduct kinesis = new BasicProduct(Product.kinesis, "Kinesis");
    public static final BasicProduct kinesis_firehose = new BasicProduct(Product.kinesis_firehose, "Kinesis Firehose");
    public static final BasicProduct kms = new BasicProduct(Product.kms, "Key Management Service");
    public static final BasicProduct lambda = new BasicProduct(Product.lambda, "Lambda");
    public static final BasicProduct rds = new BasicProduct(Product.rds, "RDS Service");
    public static final BasicProduct rds_instance = new BasicProduct(Product.rds_instance, "rds_instance");
    public static final BasicProduct redshift = new BasicProduct(Product.redshift, "Redshift");
    public static final BasicProduct rekognition = new BasicProduct(Product.rekognition, "Rekognition");
    public static final BasicProduct route53 = new BasicProduct(Product.route53, "Route 53");
    public static final BasicProduct s3 = new BasicProduct(Product.s3, "Simple Storage Service");
    public static final BasicProduct service_catalog = new BasicProduct(Product.service_catalog, "Service Catalog");
    public static final BasicProduct ses = new BasicProduct(Product.ses, "Simple Email Service");
    public static final BasicProduct simpledb = new BasicProduct(Product.simpledb, "SimpleDB");
    public static final BasicProduct sns = new BasicProduct(Product.sns, "Simple Notification Service");
    public static final BasicProduct sqs = new BasicProduct(Product.sqs, "Simple Queue Service");
    public static final BasicProduct storage_gateway = new BasicProduct(Product.storage_gateway, "Storage Gateway");
    public static final BasicProduct support = new BasicProduct(Product.support, "Support (Business)");
    public static final BasicProduct sws = new BasicProduct(Product.sws, "Simple Workflow Service");
    public static final BasicProduct vpc = new BasicProduct(Product.vpc, "Virtual Private Cloud");
    public static final BasicProduct waf = new BasicProduct(Product.waf, "WAF");
    public static final BasicProduct workspaces = new BasicProduct(Product.workspaces, "WorkSpaces");
    public static final BasicProduct monitor = new BasicProduct(Product.monitor, "monitor");

    private static BasicProduct[] products = new BasicProduct[]{
    	api_gateway, athena, cloudfront, cloudhsm, cloudtrail, codecommit, config,
    	data_pipeline, data_transfer, direct_connect, database_migration, directory, dynamodb, 
    	ebs, ec2, ec2_cloudwatch, ec2_instance, ecr, efs, eip, elasticache, elasticsearch, emr, 
    	glacier, kinesis, kinesis_firehose, kms, lambda, rds, rds_instance, redshift, rekognition, route53, 
    	s3, service_catalog, ses, simpledb, sns, sqs, storage_gateway, support, sws, vpc, waf, workspaces
    	};

    static {
        for (BasicProduct product: products) {
            productsByAwsName.put("AWS " + product.awsName, product.product);
            productsByAwsName.put("Amazon " + product.awsName, product.product);
            productsByName.put(product.product.name, product.product);
        }
        productsByAwsName.put(cloudwatch.awsName, cloudwatch.product);
        productsByName.put(cloudwatch.product.name, cloudwatch.product);
        productsByAwsName.put(monitor.awsName, monitor.product);
        productsByName.put(monitor.product.name, monitor.product);
    }

    public Product getProductByAwsName(String awsName) {
        Product product = productsByAwsName.get(awsName);
        if (product == null) {
            product = new Product(awsName);
            productsByAwsName.put(awsName, product);
            productsByName.put(awsName, product);
        }
        return product;
    }

    public Product getProductByName(String name) {
        Product product = productsByName.get(name);
        if (product == null) {
            product = new Product(name);
            productsByAwsName.put(name, product);
            productsByName.put(name, product);
        }
        return product;
    }

    public List<Product> getProducts(List<String> names) {
        List<Product> result = Lists.newArrayList();
        for (String name: names)
            result.add(productsByName.get(name));
        return result;
    }

    public Collection<Product> getProducts() {
        return productsByName.values();
    }

    private static class BasicProduct {
        private final String awsName;
        private final Product product;
        BasicProduct(Product product, String awsName) {
            this.product = product;
            this.awsName = awsName;
        }
    }
}
