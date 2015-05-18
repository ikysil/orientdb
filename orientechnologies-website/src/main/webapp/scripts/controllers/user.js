'use strict';

/**
 * @ngdoc function
 * @name webappApp.controller:AboutCtrl
 * @description
 * # AboutCtrl
 * Controller of the webappApp
 */
angular.module('webappApp')
  .controller('UserCtrl', function ($scope, User) {

    $scope.tabs = [
      {
        title: 'Profile',
        url: 'views/users/profile.html'
      },
      {
        title: 'Environment',
        url: 'views/users/environment.html'
      }]

    $scope.currentTab = $scope.tabs[0].url;

    $scope.onClickTab = function (tab) {
      $scope.currentTab = tab.url;
    }

    $scope.isActiveTab = function (tabUrl) {
      return tabUrl == $scope.currentTab;
    }

    User.whoami().then(function (user) {
      $scope.user = user;
      if (User.isMember(ORGANIZATION)) {
        $scope.tabs.push({
          title: 'Organization',
          url: 'views/users/organization.html'
        })
      }
    });
  });

angular.module('webappApp')
  .controller('UserEnvCtrl', function ($scope, User, Repo) {


    $scope.environment = {}
    $scope.connections = ['plocal', 'remote', 'memory']
    Repo.one(DEFAULT_REPO).all("milestones").getList().then(function (data) {
      $scope.milestones = data.plain();
    });
    User.whoami().then(function (user) {
      $scope.user = user;
      User.environments().then(function (data) {
        $scope.environments = data.plain();
      })
    });

    $scope.newEnv = function () {
      $scope.environment = {};
      $scope.environment.distributed = false;
      // hardcoded to be able to lookup for Milestone from id
      $scope.environment.repoName = DEFAULT_REPO;
      $scope.isNew = true;
      $scope.editing = true;

    }
    $scope.cancelSave = function () {
      $scope.environment = {};
      $scope.currentEditing
      // hardcoded to be able to lookup for Milestone from id
      $scope.isNew = false;
      $scope.editing = false;

    }
    $scope.deleteEnv = function (env) {
      User.deleteEnvironment(env).then(function (data) {
        var idx = $scope.environments.indexOf(env);
        $scope.environments.splice(idx, 1);
      });
    }
    $scope.saveEnv = function () {

      if ($scope.isNew) {
        User.addEnvironment($scope.environment).then(function (data) {

          $scope.isNew = false;
          $scope.editing = false;
          $scope.environments.push(data);
        });
      } else {
        User.changeEnvironment($scope.environment).then(function (data) {
          $scope.isNew = false;
          $scope.editing = false;
          var idx = $scope.environments.indexOf($scope.currentEditing);
          $scope.environments[idx] = data;
          $scope.currentEditing = null
          $scope.environment = {}
        });
      }

    }
    $scope.editEnv = function (env) {
      $scope.environment = angular.copy(env);
      $scope.environment.repoName = DEFAULT_REPO;
      $scope.currentEditing = env;
      $scope.isNew = false;
      $scope.editing = true;
    }

  });

angular.module('webappApp')
  .controller('ChangeEnvironmentCtrl', function ($scope, User, Repo) {

    $scope.environment = angular.copy($scope.issue.environment)

    $scope.connections = ['plocal', 'remote', 'memory']
    Repo.one(DEFAULT_REPO).all("milestones").getList().then(function (data) {
      $scope.milestones = data.plain();
    });

    $scope.save = function () {
      $scope.$emit("environment:changed", $scope.environment);
      $scope.$hide();
    }
  })
angular.module('webappApp')
  .controller('UserOrgCtrl', function ($scope, User, Repo, Organization) {

    $scope.areaEditing = false;
    Organization.all("members").getList().then(function (data) {
      $scope.members = data.plain();
    })
    Organization.all("repos").getList().then(function (data) {
      $scope.repositories = data.plain();
      if ($scope.repositories.length > 0) {
        $scope.selectedRepo = $scope.repositories[0];
      }
    })
    Organization.all("scopes").getList().then(function (data) {
      $scope.areas = data.plain();
    })

    Organization.all("bots").getList().then(function (data) {
      $scope.bots = data.plain();
    })

    Organization.all("contracts").getList().then(function (data) {
      $scope.contracts = data.plain();
    })

    Organization.all("milestones").all('current').getList().then(function (data) {
      var currents = data.plain();
      Organization.all("milestones").getList().then(function (data) {
        $scope.milestones = data.plain().map(function (m) {
          currents.forEach(function (m1) {
            if (m1.title == m.title) {
              m.current = true;
            }
          })
          return m;
        });
      })
    })


    $scope.addBot = function () {
      Organization.all("bots").one($scope.newBot).post().then(function (data) {
        $scope.bots.push(data);
      });
    }
    $scope.addRepository = function () {
      Organization.all("repos").one($scope.newRepo).post().then(function (data) {
        $scope.repositories.push(data.plain());
        $scope.newRepo = null;
      });
    }
    $scope.addArea = function () {
      $scope.areaEditing = true;
      $scope.area = {}
      $scope.area.members = [];
    }
    $scope.addContract = function () {
      $scope.contractEditing = true;
      $scope.contract = {}
      $scope.contract.businessHours = new Array(7);
    }
    $scope.saveMilestone = function (milestone) {
      Organization.all("milestones").one(encodeURI(milestone.title)).patch({current: milestone.current}).then(function (data) {

      })
    }
    $scope.cancelContract = function () {
      $scope.contractEditing = false;
      $scope.contract = {}
      $scope.contract.businessHours = new Array(7);
    }
    $scope.cancelArea = function () {
      $scope.areaEditing = false;
      $scope.area = {}
      $scope.area.members = [];
    }

    $scope.saveContract = function () {

      if (!$scope.contract.id) {
        Organization.all("contracts").post($scope.contract).then(function (data) {
          $scope.contracts.push(data);
          $scope.cancelContract();
        })
      } else {
        var idx = $scope.contracts.indexOf($scope.selectedContract);
        Organization.all("contracts").one($scope.contract.name).patch($scope.contract).then(function (data) {
          $scope.contracts[idx] = data;
          $scope.cancelContract();
        })
      }
    }
    $scope.saveArea = function () {
      if (!$scope.area.number) {
        Organization.all("scopes").post($scope.area).then(function (data) {
          $scope.areas.push(data);
          $scope.cancelArea();
        })
      } else {
        var idx = $scope.areas.indexOf($scope.selectedArea);
        Organization.all("scopes").one($scope.area.number.toString()).patch($scope.area).then(function (data) {
          $scope.areas[idx] = data;
          $scope.cancelArea();
        })
      }
    }
    $scope.toggleBackup = function (member) {
      var idx = $scope.area.members.indexOf(member.name)
      if (idx == -1) {
        $scope.area.members.push(member.name);
      } else {
        $scope.area.members.splice(idx, 1);
      }
    }
    $scope.selectArea = function (area) {
      $scope.selectedArea = area;
      $scope.area = angular.copy(area);
      $scope.area.members = $scope.area.members.map(function (e) {
        return e.name;
      });
      if (area.owner) {
        $scope.area.owner = area.owner.name;
      }
      $scope.area.repository = area.repository.name;
      $scope.areaEditing = true;
    }
    $scope.selectContract = function (contract) {
      $scope.selectedContract = contract;
      $scope.contract = angular.copy(contract);
      $scope.contractEditing = true;
    }
  });


angular.module('webappApp')
  .controller('UserProfileCtrl', function ($scope, User, Repo) {


    $scope.message = 'Please fill this information to use PrjHub';
    User.whoami().then(function (user) {
      $scope.user = user;

      $scope.member = User.isMember(ORGANIZATION);
      $scope.isClient = User.isClient(ORGANIZATION);
      $scope.viewMessage = !$scope.user.confirmed && !$scope.member && !$scope.isClient;
    });

    $scope.save = function () {

      User.save($scope.user).then(function (data) {

        var jacked = humane.create({baseCls: 'humane-jackedup', addnCls: 'humane-jackedup-success'})
        jacked.log("Profile saved.");
      });
    }
  })
angular.module('webappApp')
  .controller('ChangeSelectEnvironmentCtrl', function ($scope, User, Repo) {


    User.whoami().then(function (user) {
      $scope.user = user;
      User.environments().then(function (data) {
        $scope.environments = data.plain();
      })
    });

    $scope.save = function () {
      $scope.$emit("environment:changed", $scope.environment);
      $scope.$hide();
    }
  })
