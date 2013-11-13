'use strict';

angular.module('MonitorApp')
    .controller('NavController', function ($scope, $routeParams, $location, Login) {


        $scope.login = Login;


        Login.current();
        $scope.menus = [
            {name: 'dashboard', i18n: 'dashboard', link: '#/dashboard/', icon: 'icon-dashboard' },
            {name: 'settings', i18n: 'dashboard.settings', link: '#/dashboard/settings/', icon: 'icon-gear' },
            {name: 'general', i18n: 'dashboard.general', link: '#/dashboard/general/', icon: 'icon-cogs' },
            {name: 'query', i18n: 'dashboard.query', link: '#/dashboard/query', icon: 'icon-rocket' },
            {name: 'metrics', i18n: 'dashboard.metrics', link: '#/dashboard/metrics/' + $scope.rid, icon: 'icon-bar-chart' },
            {name: 'logjava', i18n: 'dashboard.javalog', link: '#/dashboard/logjava/', icon: 'icon-bug' },
            {name: 'events', i18n: 'dashboard.events', link: '#/dashboard/events/', icon: 'icon-bell' },
            {name: 'messages', i18n: 'dashboard.messages', link: '#/dashboard/messages/', icon: 'icon-envelope' }


        ]


        $scope.setSelected = function () {

            $scope.menus.forEach(function (element, index, array) {
                var find = $location.path().indexOf("/" + element.name.toLowerCase());
                if (find != -1) {
                    $scope.selectedMenu = element;
                }

            });
        }
        $scope.navigate = function (menu) {
            var menuEntry = menu.name != 'dashboard' ? (menu.name) : "";
            $location.path('/dashboard/' + menuEntry);
        }
        $scope.$on('$routeChangeSuccess', function (scope, next, current) {
            $scope.setSelected();
        });

        $scope.logout = function () {
            Login.logout();
        }
    });