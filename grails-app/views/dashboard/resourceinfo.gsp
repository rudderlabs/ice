<%--

    Copyright 2013 Netflix, Inc.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.

--%>

<%@ page contentType="text/html;charset=UTF-8" %>
<head>
  <meta http-equiv="Content-Type" content="text/html; charset=UTF-8"/>
  <meta name="layout" content="main"/>
  <title>Resource Info</title>
</head>
<body>
<div class="" style="margin: auto; width: 1200px; padding: 20px 30px" ng-controller="resourceInfoCtrl">

  <div class="message" ng-show="message">{{message}}</div>

  <div class="resourceColumns">
    <div class="resourceInfo">
		  <h1>Resource Information</h1>
		
		  <div class="dialog">
		    <table>
		      <tr class="prop">
		        <td class="name">Resource ID/Name:</td>
		        <td class="name"><input ng-model="resource.name" class="resourceName" type="text"/></td>
		        <td></td>
		      </tr>
		    </table>
		    <div class="buttons">
		      <span ng-show="loading"><img src="${resource(dir: "/")}images/spinner.gif" border="0"></span>
		      <button ng-show="!loading" ng-disabled="isDisabled()" type="submit" class="save" ng-click="getResourceInfo()"><div>Submit</div></button>
		    </div>
		  </div>
		  <div ng-show"resourceInfo" class="resourceData"><pre>{{resourceInfo}}</pre></div>
    </div>
    <div class="resourceExamples">
		  <h1>Resource ID Format</h1>
      <table>
        <tr><th>Resource/Product</th><th>ID</th></tr>
        <tr><th>API Gateway</th><th>ARN</th></tr>
        <tr><th>Athena</th><th>ARN</th></tr>
        <tr><th>CloudFront</th><th>ARN</th></tr>
        <tr><th>Cognito</th><th>ARN</th></tr>
        <tr><th>DataPipeline</th><th>ARN</th></tr>
        <tr><th>Database Migration Service</th><th>ARN</th></tr>
        <tr><th>Direct Connect</th><th>ID (e.g. dxvif-fgyuq8nz)</th></tr>
        <tr><th>Directory Service</th><th>ARN</th></tr>
        <tr><th>DocumentDB</th><th>ARN</th></tr>
        <tr><th>DynamoDB</th><th>ARN</th></tr>
        <tr><th>EC2 NatGateway</th><th>ARN</th></tr>
        <tr><th>EC2 ELB</th><th>ARN</th></tr>
        <tr><th>EC2 Container Registry</th><th>ARN</th></tr>
        <tr><th>EC2 Container Service</th><th>ARN</th></tr>
        <tr><th>EC2 Instance</th><th>ID (e.g. i-00003abcd1236cf12)</th></tr>
        <tr><th>EBS Snapshot</th><th>ARN</th></tr>
        <tr><th>EBS Volume</th><th>ID (e.g. vol-028e12f67df0f34ab)</th></tr>
        <tr><th>Elastic File System</th><th>ARN</th></tr>
        <tr><th>Elastic Map Reduce</th><th>ARN</th></tr>
        <tr><th>ElasticCache</th><th>ARN</th></tr>
        <tr><th>Elasticsearch Service</th><th>ARN</th></tr>
        <tr><th>Glacier</th><th>ARN</th></tr>
        <tr><th>Glue</th><th>ARN</th></tr>
        <tr><th>Lambda</th><th>ARN</th></tr>
        <tr><th>RDS Instance</th><th>ARN</th></tr>
        <tr><th>Route53</th><th>ARN</th></tr>
        <tr><th>S3</th><th>Bucket Name</th></tr>
        <tr><th>Simple Queue Service</th><th>ARN</th></tr>
        <tr><th>Transfer for SFTP</th><th>ARN</th></tr>
        <tr><th>Virtual Private Cloud</th><th>ID (e.g. vpn-01abcdef)</th></tr>
        <tr><th>WAF</th><th>ARN</th></tr>
        <tr><th>WorkSpaces</th><th>ARN</th></tr>
      </table>
    </div>
  </div>
</div>
</body>
</html>