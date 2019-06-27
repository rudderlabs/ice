package com.netflix.ice.processor.kubernetes;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.netflix.ice.common.AwsUtils;
import com.netflix.ice.common.TagGroup;
import com.netflix.ice.processor.CostAndUsageData;
import com.netflix.ice.processor.ProcessorConfig;
import com.netflix.ice.processor.ReadWriteData;
import com.netflix.ice.processor.config.KubernetesConfig;
import com.netflix.ice.processor.kubernetes.KubernetesReport.KubernetesColumn;
import com.netflix.ice.tag.Product;
import com.netflix.ice.tag.ResourceGroup;
import com.netflix.ice.tag.UserTag;

public class KubernetesProcessor {
    protected Logger logger = LoggerFactory.getLogger(getClass());
    
    public static final String reportPrefix = "kubernetes-";
    public static final double vCpuToMemoryCostRatio = 10.9;
    
    protected final ProcessorConfig config;
    private final List<KubernetesReport> reports;

	public KubernetesProcessor(ProcessorConfig config, DateTime start) throws IOException {
		this.config = config;
		
		List<KubernetesReport> reports = null;
		reports = getReportsToProcess(start);
		this.reports = reports;
		
	}	
	
	protected List<KubernetesReport> getReportsToProcess(DateTime start) throws IOException {
        List<KubernetesReport> filesToProcess = Lists.newArrayList();

        // list the kubernetes report files in the kubernetes buckets
        for (int i = 0; i < config.kubernetesS3BucketNames.length; i++) {
            String kubernetesS3BucketName = config.kubernetesS3BucketNames[i];
            if (kubernetesS3BucketName.isEmpty())
            	continue;
            
            String kubernetesS3BucketRegion = config.kubernetesS3BucketRegions.length > i ? config.kubernetesS3BucketRegions[i] : "";
            String kubernetesS3BucketPrefix = config.kubernetesS3BucketPrefixes.length > i ? config.kubernetesS3BucketPrefixes[i] : "";
            String accountId = config.kubernetesAccountIds.length > i ? config.kubernetesAccountIds[i] : "";
            String kubernetesAccessRoleName = config.kubernetesAccessRoleNames.length > i ? config.kubernetesAccessRoleNames[i] : "";
            String kubernetesAccessExternalId = config.kubernetesAccessExternalIds.length > i ? config.kubernetesAccessExternalIds[i] : "";
            
            String prefix = kubernetesS3BucketPrefix;
            if (!prefix.isEmpty() && !prefix.endsWith("/"))
            	prefix += "/";

            String fileKey = prefix + reportPrefix + AwsUtils.monthDateFormat.print(start);

            logger.info("trying to list objects in kubernetes bucket " + kubernetesS3BucketName +
            		" using assume role \"" + accountId + ":" + kubernetesAccessRoleName + "\", and external id \"" + kubernetesAccessExternalId + "\" with key " + fileKey);
            
            List<S3ObjectSummary> objectSummaries = AwsUtils.listAllObjects(kubernetesS3BucketName, kubernetesS3BucketRegion, fileKey,
                    accountId, kubernetesAccessRoleName, kubernetesAccessExternalId);
            logger.info("found " + objectSummaries.size() + " in billing bucket " + kubernetesS3BucketName);
            
            if (objectSummaries.size() > 0) {
            	// Find the matching configuration data for this Kubernetes report
            	KubernetesConfig kubernetesConfig = null;
            	for (KubernetesConfig kc: config.kubernetesConfigs) {
            		logger.info("Bucket " + kubernetesS3BucketName + ", " + kc.getBucket() + ", Prefix " + kubernetesS3BucketPrefix + ", " + kc.getPrefix());
            		if (kubernetesS3BucketName.equals(kc.getBucket()) && kubernetesS3BucketPrefix.equals(kc.getPrefix())) {
            			kubernetesConfig = kc;
            			break;
            		}
            	}
            	if (kubernetesConfig != null) {
	            	filesToProcess.add(new KubernetesReport(objectSummaries.get(0), kubernetesS3BucketRegion, accountId,
	            			kubernetesAccessRoleName, kubernetesAccessExternalId, prefix, start, kubernetesConfig, config.resourceService));
            	}
            }
        }

        return filesToProcess;
	}

	
	public void downloadAndProcessReports(CostAndUsageData data) throws Exception {
		if (reports == null || reports.isEmpty()) {
			logger.info("No kubernetes reports to process");
			return;
		}
		
		for (KubernetesReport report: reports) {
			report.loadReport(config.localDir);
		}
		
		String[] products = new String[]{
			Product.ec2Instance,
			Product.cloudWatch,
			Product.ebs,
			Product.dataTransfer,
		};
		
		for (String productName: products) {
			Product p = config.productService.getProductByName(productName);
			logger.info("Process kubernetes data for " + productName);
			process(data.getCost(p));
		}
	}
	
	protected void process(ReadWriteData costData) {
		// Process each hour of data
		for (int i = 0; i < costData.getNum(); i++) {
			Map<TagGroup, Double> hourCostData = costData.getData(i);
			// Get a copy of the key set since we'll be updating the map
			Set<TagGroup> tagGroups = Sets.newHashSet(hourCostData.keySet());
			for (TagGroup tg: tagGroups) {
				if (tg.resourceGroup == null || tg.resourceGroup.isProductName())
					continue;
				
				UserTag[] ut = tg.resourceGroup.getUserTags();
								
				for (KubernetesReport report: reports) {
					if (report.isCompute(ut)) {
						List<String> clusterNames = report.getClusterNameBuilder().getClusterNames(ut);
						if (clusterNames.size() > 0) {
							for (String clusterName: clusterNames) {
								List<String[]> hourClusterData = report.getData(clusterName, i);
								if (hourClusterData != null) {
									processHourClusterData(hourCostData, tg, clusterName, report, hourClusterData);
								}
							}
						}
					}
				}					
			}
		}
	}
		
	protected void processHourClusterData(Map<TagGroup, Double> hourCostData, TagGroup tg, String cluster, KubernetesReport report, List<String[]> hourClusterData) {		
		Double totalCost = hourCostData.get(tg);
		if (totalCost == null)
			return;
		
		int namespaceIndex = report.getNamespaceIndex();
		double unusedCost = totalCost;
		Tagger tagger = report.getTagger();
		for (String[] item: hourClusterData) {
			double allocatedCost = getAllocatedCost(tg, totalCost, report, item);
			if (allocatedCost == 0.0)
				continue;
			
			UserTag[] userTags = tg.resourceGroup.getUserTags().clone();
			
			String namespace = report.getString(item, KubernetesColumn.Namespace);
			if (namespaceIndex >= 0)
				userTags[namespaceIndex] = UserTag.get(namespace);
			
			if (tagger != null)
				tagger.tag(report, item, userTags);
			
			TagGroup allocated = TagGroup.getTagGroup(tg.account, tg.region, tg.zone, tg.product, tg.operation, tg.usageType, ResourceGroup.getResourceGroup(userTags));
			
			hourCostData.put(allocated,  allocatedCost);
			
			unusedCost -= allocatedCost;
		}
		
		// Put the remaining cost on the original tagGroup with namespace set to "unused"
		UserTag[] userTags = tg.resourceGroup.getUserTags().clone();
		userTags[namespaceIndex] = UserTag.get("unused");
		TagGroup unused = TagGroup.getTagGroup(tg.account, tg.region, tg.zone, tg.product, tg.operation, tg.usageType, ResourceGroup.getResourceGroup(userTags));
		hourCostData.remove(tg);
		hourCostData.put(unused, unusedCost);
	}
	
	private double getAllocatedCost(TagGroup tg, double cost, KubernetesReport report, String[] item) {
		String productName = tg.product.name;
		if (productName.equals(Product.ec2Instance) || productName.equals(Product.cloudWatch)) {
			double cpuCores = report.getDouble(item, KubernetesColumn.RequestsCPUCores);
			double clusterCores = report.getDouble(item, KubernetesColumn.ClusterCPUCores);
			double memoryGiB = report.getDouble(item, KubernetesColumn.RequestsMemoryGiB);
			double clusterMemoryGiB = report.getDouble(item, KubernetesColumn.ClusterMemoryGiB);
			double unitsPerCluster = clusterCores * vCpuToMemoryCostRatio + clusterMemoryGiB;
			double ratePerUnit = cost / unitsPerCluster;
			return ratePerUnit * (cpuCores * vCpuToMemoryCostRatio + memoryGiB);
		}
		else if (productName.equals(Product.ebs)) {
			double pvcGiB = report.getDouble(item, KubernetesColumn.PersistentVolumeClaimGiB);
			double clusterPvcGiB = report.getDouble(item, KubernetesColumn.ClusterPersistentVolumeClaimGiB);
			return cost * pvcGiB / clusterPvcGiB;
		}
		else if (productName.equals(Product.dataTransfer)) {
			double networkGiB = report.getDouble(item, KubernetesColumn.NetworkInGiB) + report.getDouble(item, KubernetesColumn.NetworkOutGiB);
			double clusterNetworkGiB = report.getDouble(item, KubernetesColumn.ClusterNetworkInGiB) + report.getDouble(item, KubernetesColumn.ClusterNetworkOutGiB);
			return cost * networkGiB / clusterNetworkGiB;
		}
		return 0;
	}
	
}
