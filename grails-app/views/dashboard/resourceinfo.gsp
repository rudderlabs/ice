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

  <h1>Resource Information</h1>

  <div class="dialog">
    <table>
      <tr class="prop">
        <td class="name">Resource ID/Name:</td>
        <td class="name">
        	<form ng-submit="getResourceInfo()">
        		<input ng-model="resource.name" class="resourceName" type="text" ngSubmit="getResourceInfo()"/>
        	</form>
        </td>
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
</body>
</html>