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
<%@ page import="com.netflix.ice.reader.ReaderConfig" %>
<head>
  <meta http-equiv="Content-Type" content="text/html; charset=UTF-8"/>
  <meta name="layout" content="main"/>
  <title>Processor Status</title>
</head>
<body>
<div class="list" style="margin: auto; width: 1200px; padding: 20px 30px" ng-controller="processorStatusCtrl">
  <h1>Processor Status</h1>
  <table>
	<thead>
	  <tr>
		<th>Month</th>
		<th>Last Processed</th>
		<th>Reprocess</th>
		<th>Report</th>
		<th>Last Modified</th>
	  </tr>
	</thead>
	<tbody>
	  <g:set var="odd" value="${0}"/>
	  <g:each in="${ReaderConfig.getInstance().managers.getProcessorStatus()}" var="status">
	    <g:set var="first" value="${true}"/>
	    <g:each in="${status.reports}" var="report">
	      <tr class="${odd == 0 ? 'even' : 'odd'}" foo="${odd}">
	        <g:if test="${first}">
	          <g:set var="first" value="${false}"/>
		      <td rowspan="${status.reports.size()}">${status.month}</td>
		      <td rowspan="${status.reports.size()}">${status.lastProcessed}</td>
		      <td rowspan="${status.reports.size()}">${status.reprocess}</td>
	        </g:if>
		    <td>${report.key}</td>
		    <td>${report.lastModified}</td>
	      </tr>
	    </g:each>
	    <g:set var="odd" value="${odd ^ 1}"/>
	  </g:each>
	</tbody>
  </table>
</div>
</body>
</html>