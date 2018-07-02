<%@ page contentType="text/html;charset=UTF-8" %>
<head>
  <meta http-equiv="Content-Type" content="text/html; charset=UTF-8"/>
  <meta name="layout" content="main"/>
  <title>Aws Tag Coverage</title>
</head>
<body>
<div class="" style="margin: auto; width: 1600px; padding: 20px 30px" ng-controller="tagCoverageCtrl">

  <table>
    <tr>
      <td>Start</td>
      <td>Show</td>
      <td ng-show="isGroupByTagKey() || !showUserTags">TagKey</td>
      <td>Account</td>
      <td>Region</td>
      <td>Product</td>
      <td>Operation</td>
      <td>UsageType</td>
    </tr>
    <tr>
      <td>
        <input class="required" type="text" name="start" id="start" size="14"/>
        <div style="padding-top: 10px">End</div>
        <br><input class="required" type="text" name="end" id="end" size="14"/>
      </td>
      <td nowrap="">
        <div style="padding-top: 10px">Group by
          <select ng-model="groupBy" ng-options="a.name for a in groupBys"></select>
        </div>
        <div style="padding-top: 5px" ng-show="throughput_metricname">
          <input type="checkbox" ng-model="showsps" id="showsps">
          <label for="showsps">Show {{throughput_metricname}}</label>
        </div>
        <div style="padding-top: 5px" ng-show="throughput_metricname">
          <input type="checkbox" ng-model="factorsps" id="factorsps">
          <label for="factorsps">Factor {{throughput_metricname}}</label>
        </div>
      </td>
      <td ng-show="isGroupByTagKey()">
        <select ng-model="selected_tags" ng-options="a.name for a in tags | filter:filter_tags" multiple="multiple" class="metaTags metaSelect"></select>
        <br><input ng-model="filter_tags" type="text" class="metaFilter" placeholder="filter">
        <button ng-click="selected_tags = tags" class="allNoneButton">+</button>
        <button ng-click="selected_tags = []" class="allNoneButton">-</button>
      </td>
      <td ng-show="!isGroupByTagKey()">
      	<div  style="padding-top: 10px">
        	<select ng-model="selected_tag" ng-options="a.name for a in tags"></select>
        </div>
      </td>      
      <td>
        <select ng-model="selected_accounts" ng-options="a.name for a in accounts | filter:filter_accounts" ng-change="accountsChanged()" multiple="multiple" class="metaAccounts metaSelect"></select>
        <br><input ng-model="filter_accounts" type="text" class="metaFilter" placeholder="filter">
        <button ng-click="selected_accounts = accounts" class="allNoneButton">+</button>
        <button ng-click="selected_accounts = []" class="allNoneButton">-</button>
      </td>
      <td>
        <select ng-model="selected_regions" ng-options="a.name for a in regions | filter:filter_regions" ng-change="regionsChanged()" multiple="multiple" class="metaRegions metaSelect"></select>
        <br><input ng-model="filter_regions" type="text" class="metaFilter" placeholder="filter">
        <button ng-click="selected_regions = regions" class="allNoneButton">+</button>
        <button ng-click="selected_regions = []" class="allNoneButton">-</button>
      </td>
      <td>
        <select ng-model="selected_products" ng-options="a.name for a in products | filter:filter_products" ng-change="productsChanged()" multiple="multiple" class="metaProducts metaSelect"></select>
        <br><input ng-model="filter_products" type="text" class="metaFilter" placeholder="filter">
        <button ng-click="selected_products = products" class="allNoneButton">+</button>
        <button ng-click="selected_products = []" class="allNoneButton">-</button>
      </td>
      <td>
        <select ng-model="selected_operations" ng-options="a.name for a in operations | filter:filter_operations" ng-change="operationsChanged()" multiple="multiple" class="metaOperations metaSelect"></select>
        <br><input ng-model="filter_operations" type="text" class="metaFilter" placeholder="filter">
        <button ng-click="selected_operations = operations" class="allNoneButton">+</button>
        <button ng-click="selected_operations = []" class="allNoneButton">-</button>
      </td>
      <td>
        <select ng-model="selected_usageTypes" ng-options="a.name for a in usageTypes | filter:filter_usageTypes" multiple="multiple" class="metaUsageTypes metaSelect"></select>
        <br><input ng-model="filter_usageTypes" type="text" class="metaFilter" placeholder="filter">
        <button ng-click="selected_usageTypes = usageTypes" class="allNoneButton">+</button>
        <button ng-click="selected_usageTypes = []" class="allNoneButton">-</button>
      </td>

    </tr>
  </table>
  <table ng-show="showUserTags" class="userTags">
    <tr ng-show="userTagValues.length > 0">
      <td>
   	    Tags:
        <div ng-show="!isGroupByTagKey()" style="padding-top: 10px">Group by
          <select ng-model="groupByTag" ng-options="a.name for a in groupByTags"></select>
        </div>      
      </td>
      <td ng-show="userTagValues.length > 0">
        <input type="checkbox" ng-model="enabledUserTags[0]"> {{userTags[0].name}}</input>
      	<div ng-show="enabledUserTags[0]">
          <select ng-model="selected_userTagValues[0]" ng-options="a.name for a in userTagValues[0] | filter:filter_userTagValues[0]" ng-change="resourceGroupsChanged()" multiple="multiple" class="metaUserTags metaSelect"></select>
          <br><input ng-model="filter_userTagValues[0]" type="text" class="metaFilter" placeholder="filter">
		</div>      
      </td>
      <td ng-show="userTagValues.length > 1">
        <input type="checkbox" ng-model="enabledUserTags[1]"> {{userTags[1].name}}</input>
      	<div ng-show="enabledUserTags[1]">
          <select ng-model="selected_userTagValues[1]" ng-options="a.name for a in userTagValues[1] | filter:filter_userTagValues[1]" ng-change="resourceGroupsChanged()" multiple="multiple" class="metaUserTags metaSelect"></select>
          <br><input ng-model="filter_userTagValues[1]" type="text" class="metaFilter" placeholder="filter">
		</div>      
      </td>
      <td ng-show="userTagValues.length > 2">
        <input type="checkbox" ng-model="enabledUserTags[2]"> {{userTags[2].name}}</input>
      	<div ng-show="enabledUserTags[2]">
          <select ng-model="selected_userTagValues[2]" ng-options="a.name for a in userTagValues[2] | filter:filter_userTagValues[2]" ng-change="resourceGroupsChanged()" multiple="multiple" class="metaUserTags metaSelect"></select>
          <br><input ng-model="filter_userTagValues[2]" type="text" class="metaFilter" placeholder="filter">
		</div>      
      </td>
      <td ng-show="userTagValues.length > 3">
        <input type="checkbox" ng-model="enabledUserTags[3]"> {{userTags[3].name}}</input>
      	<div ng-show="enabledUserTags[3]">
          <select ng-model="selected_userTagValues[3]" ng-options="a.name for a in userTagValues[3] | filter:filter_userTagValues[3]" ng-change="resourceGroupsChanged()" multiple="multiple" class="metaUserTags metaSelect"></select>
          <br><input ng-model="filter_userTagValues[3]" type="text" class="metaFilter" placeholder="filter">
		</div>      
      </td>
      <td ng-show="userTagValues.length > 4">
        <input type="checkbox" ng-model="enabledUserTags[4]"> {{userTags[4].name}}</input>
      	<div ng-show="enabledUserTags[4]">
          <select ng-model="selected_userTagValues[4]" ng-options="a.name for a in userTagValues[4] | filter:filter_userTagValues[4]" ng-change="resourceGroupsChanged()" multiple="multiple" class="metaUserTags metaSelect"></select>
          <br><input ng-model="filter_userTagValues[4]" type="text" class="metaFilter" placeholder="filter">
		</div>      
      </td>
      <td ng-show="userTagValues.length > 5">
        <input type="checkbox" ng-model="enabledUserTags[5]"> {{userTags[5].name}}</input>
      	<div ng-show="enabledUserTags[5]">
          <select ng-model="selected_userTagValues[5]" ng-options="a.name for a in userTagValues[5] | filter:filter_userTagValues[5]" ng-change="resourceGroupsChanged()" multiple="multiple" class="metaUserTags metaSelect"></select>
          <br><input ng-model="filter_userTagValues[5]" type="text" class="metaFilter" placeholder="filter">
		</div>      
      </td>
    </tr>
    <tr ng-show="userTagValues.length > 6">
      <td></td>
      <td ng-show="userTagValues.length > 6">
      	<input type="checkbox" ng-model="enabledUserTags[6]"> {{userTags[6].name}}</input>
      	<div ng-show="enabledUserTags[6]">
	      <select ng-model="selected_userTagValues[6]" ng-options="a.name for a in userTagValues[6] | filter:filter_userTagValues[6]" ng-change="resourceGroupsChanged()" multiple="multiple" class="metaUserTags metaSelect"></select>
	      <br><input ng-model="filter_userTagValues[6]" type="text" class="metaFilter" placeholder="filter">
		</div>      
	  </td>
      <td ng-show="userTagValues.length > 7">
      	<input type="checkbox" ng-model="enabledUserTags[7]"> {{userTags[7].name}}</input>
      	<div ng-show="enabledUserTags[7]">
          <select ng-model="selected_userTagValues[7]" ng-options="a.name for a in userTagValues[7] | filter:filter_userTagValues[7]" ng-change="resourceGroupsChanged()" multiple="multiple" class="metaUserTags metaSelect"></select>
          <br><input ng-model="filter_userTagValues[7]" type="text" class="metaFilter" placeholder="filter">
		</div>      
      </td>
      <td ng-show="userTagValues.length > 8">
      	<input type="checkbox" ng-model="enabledUserTags[8]"> {{userTags[8].name}}</input>
      	<div ng-show="enabledUserTags[8]">
          <select ng-model="selected_userTagValues[8]" ng-options="a.name for a in userTagValues[8] | filter:filter_userTagValues[8]" ng-change="resourceGroupsChanged()" multiple="multiple" class="metaUserTags metaSelect"></select>
          <br><input ng-model="filter_userTagValues[8]" type="text" class="metaFilter" placeholder="filter">
		</div>      
      </td>
      <td ng-show="userTagValues.length > 9">
      	<input type="checkbox" ng-model="enabledUserTags[9]"> {{userTags[9].name}}</input>
      	<div ng-show="enabledUserTags[9]">
          <select ng-model="selected_userTagValues[9]" ng-options="a.name for a in userTagValues[9] | filter:filter_userTagValues[9]" ng-change="resourceGroupsChanged()" multiple="multiple" class="metaUserTags metaSelect"></select>
          <br><input ng-model="filter_userTagValues[9]" type="text" class="metaFilter" placeholder="filter">
		</div>      
      </td>
      <td ng-show="userTagValues.length > 10">
      	<input type="checkbox" ng-model="enabledUserTags[10]"> {{userTags[10].name}}</input>
      	<div ng-show="enabledUserTags[10]">
          <select ng-model="selected_userTagValues[10]" ng-options="a.name for a in userTagValues[10] | filter:filter_userTagValues[10]" ng-change="resourceGroupsChanged()" multiple="multiple" class="metaUserTags metaSelect"></select>
          <br><input ng-model="filter_userTagValues[10]" type="text" class="metaFilter" placeholder="filter">
		</div>      
      </td>
      <td ng-show="userTagValues.length > 11">
      	<input type="checkbox" ng-model="enabledUserTags[11]"> {{userTags[11].name}}</input>
      	<div ng-show="enabledUserTags[11]">
          <select ng-model="selected_userTagValues[11]" ng-options="a.name for a in userTagValues[11] | filter:filter_userTagValues[11]" ng-change="resourceGroupsChanged()" multiple="multiple" class="metaUserTags metaSelect"></select>
          <br><input ng-model="filter_userTagValues[11]" type="text" class="metaFilter" placeholder="filter">
		</div>      
      </td>
    </tr>
  </table>

  <div class="buttons">
    <img src="${resource(dir: '/')}images/spinner.gif" ng-show="loading">
    <a href="javascript:void(0)" class="monitor" style="background-image: url(${resource(dir: '/')}images/tango/16/apps/utilities-system-monitor.png)"
       ng-click="updateUrl(); getData()" ng-show="!loading"
       ng-disabled="selected_tags.length == 0 || selected_accounts.length == 0 || selected_regions.length == 0 && !showZones || selected_zones.length == 0 && showZones || selected_products.length == 0 || selected_operations.length == 0 || selected_usageTypes.length == 0">Submit</a>
    <a href="javascript:void(0)" style="background-image: url(${resource(dir: '/')}images/tango/16/actions/document-save.png)"
       ng-click="download()" ng-show="!loading"
       ng-disabled="selected_tags.length == 0 || selected_accounts.length == 0 || selected_regions.length == 0 && !showZones || selected_zones.length == 0 && showZones || selected_products.length == 0 || selected_operations.length == 0 || selected_usageTypes.length == 0">Download</a>
  </div>

  <table style="width: 100%; margin-top: 20px">
    <tr>
      <td>
        <div>
          <a href="javascript:void(0)" class="legendControls" ng-click="showall()">SHOW ALL</a>
          <a href="javascript:void(0)" class="legendControls" ng-click="hideall()">HIDE ALL</a>
          <input ng-model="filter_legend" type="text" class="metaFilter" placeHolder="filter" style="float: right; margin-right: 0">
        </div>
        <div class="list">
          <table style="width: 100%;">
            <thead>
            <tr>
              <th ng-click="order(legends, 'name', false)">{{legendName}}</th>
              <th ng-click="order(legends, 'max', true)">Max</th>
              <th ng-click="order(legends, 'avgerage', true)">Avg</th>
              <th ng-click="order(legends, 'min', true)">Min</th>
            </tr>
            </thead>
            <tbody>
            <tr ng-repeat="legend in legends | filter:filter_legend" style="{{legend.style}}; cursor: pointer;" ng-click="clickitem(legend)" class="{{getTrClass($index)}}">
              <td style="word-wrap: break-word"><div class="legendIcon" style="{{legend.iconStyle}}"></div>{{legend.name}}</td>
              <td>{{legend.stats.max | number:legendPrecision}}%</td>
              <td>{{legend.stats.average | number:legendPrecision}}%</td>
              <td>{{legend.stats.min | number:legendPrecision}}%</td>
            </tr>
            </tbody>
          </table>
        </div>
      </td>
      <td style="width: 65%">
        <div id="highchart_container" style="width: 100%; height: 600px;">
        </div>
      </td>
    </tr>
  </table>

</div>
</body>
</html>