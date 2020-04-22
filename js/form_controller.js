var app = angular.module('warpscriptplugin.module', []);

app.controller('Warp10FormController', function($scope, $stateParams, CodeMirrorSettingService) {
    $scope.paramDesc = {'parameterSetId': 'warp10connection',
                        'mandatory': true};
    $scope.editorOptions = CodeMirrorSettingService.get("text/plain");

    $scope.init = function() {
        DataikuAPI.plugins.listAccessiblePresets('warp10', $stateParams.projectKey, 'warp10connection').success(function (data) {
            $scope.inlineParams = data.inlineParams;
            $scope.inlinePluginParams = data.inlinePluginParams;
            $scope.accessiblePresets = [];
            if (data.definableInline) {
                $scope.accessiblePresets.push({
                    name:"INLINE",
                    label:"Manually defined", usable:true,
                    description: "Define values for these parameters"
                });
            }
            data.presets.forEach(function(p) {
                $scope.accessiblePresets.push({name:"PRESET " + p.name, label:p.name, usable:p.usable, description:p.description});
            });
            $scope.accessibleParameterSetDescriptions = $scope.accessiblePresets.map(function(p) { 
                return p.description || '<em>No description</em>';
            });
        }).error(setErrorInScope.bind($scope.errorScope));
    };
});

app.controller('WarpScriptFormController', function($scope, CodeMirrorSettingService) {
    $scope.editorOptions = CodeMirrorSettingService.get("text/plain");

    $scope.accessibleColumnsList = $scope.columnsPerInputRole.input.map(column => column.name);
    $scope.$on('computablesMapChanged', function() {
        // Not sure about this, but sometimes the input list is not built yet.
        $scope.accessibleColumnsList = $scope.columnsPerInputRole.input.map(column => column.name);
    });
});

