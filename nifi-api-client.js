var fs = require('fs');
var path = require('path');
var request = require('request');
var yaml = require('js-yaml');

var fromYamlFile = function(filepath, configName) {
  var conf = yaml.safeLoad(fs.readFileSync(filepath));
  if (typeof(configName) === 'string') {
    conf = conf[configName];
  }
  return new NiFiApi(conf);
}

var NiFiApi = function(conf) {
  this.conf = conf;
  this.checkServerIdentity = true;
  if (conf.secure) {
    if (conf.caCert) this.caCert = fs.readFileSync(conf.caCert);
    if (conf.clientCert) this.clientCert = fs.readFileSync(conf.clientCert);
    if (conf.clientKey) this.clientKey = fs.readFileSync(conf.clientKey);
    if (conf.clientKeyPass) this.clientKeyPass = conf.clientKeyPass;
    if (typeof(conf.checkServerIdentity) === 'boolean') this.checkServerIdentity = conf.checkServerIdentity;
  }

  this.debug = function(msg, obj) {
    if (this.conf.debug) console.log(JSON.stringify([new Date(), msg, obj]));
  }

  this.getApiRoot = function() {
    return (conf.secure ? 'https' : 'http') + '://'
      + conf.host
      + (conf.port ? ':' + conf.port : '')
      + '/nifi-api';
  }

  this.request = function(options, callback) {

    var o = {};
    if (typeof(options) === 'string') {
      // Only url is specified.
      o.url = this.getApiRoot() + options;
      o.method = 'GET';
    } else {
      // Initialize with the specified options.
      for (k in options) {
        o[k] = options[k];
      }
      o.url = this.getApiRoot() + options.url;
    }

    if (this.conf.secure) {
      o.ca = this.caCert;
      o.cert = this.clientCert;
      o.key = this.clientKey;
      o.passphrase = this.clientKeyPass;
      if (!this.checkServerIdentity) {
        o.checkServerIdentity = (host, cert) => {};
      }
    }

    return request(o, callback);
  }

  // Basic APIs.
  this.searchComponent = searchComponent;
  this.getComponent = getComponent;
  this.postComponent = postComponent;
  this.putComponent = putComponent;
  this.deleteComponent = deleteComponent;

  // Templates.  
  this.uploadTemplate = uploadTemplate;
  this.instanciateTemplate = instanciateTemplate;

  // Process Groups.
  this.updateProcessGroupState = updateProcessGroupState;
  this.getProcessGroupFlow = getProcessGroupFlow;
  this.putProcessGroupFlow = putProcessGroupFlow;
  this.getProcessGroup = getProcessGroup;
  this.putProcessGroup = putProcessGroup;
  this.moveProcessGroup = moveProcessGroup;

  this.findInputPortIdByName = findInputPortIdByName;
  this.findOutputPortIdByName = findOutputPortIdByName;

  // Processors.
  this.updateProcessorState = updateProcessorState;
  this.getProcessor = getProcessor;
  this.putProcessor = putProcessor;

  // Connections.
  this.putConnection = putConnection;
  this.switchSourceConnection = switchSourceConnection;
  this.createConnection = createConnection;
}

var uploadTemplate = function(pgId, filepath, callback) {
  var req = this.request({
    url: '/process-groups/' + pgId + '/templates/upload',
    method: 'POST'
  }, (err, res, body) => {
    if (err) {
      callback(err);
      return;
    }
    this.debug('uploadTemplate', {statusCode: res.statusCode});
    switch (res.statusCode) {
      case 201:
        var match = /templates\/(.*)/.exec(res.headers.location);
        return callback(null, match[1]);
      case 200:
        // NiFi returns error message.
        return callback(res.body);
      default:
        return callback(res);
    }
  });

  var form = req.form();
  form.append('template', fs.createReadStream(filepath));
}

var instanciateTemplate = function(pgId, templateId, callback) {
  this.request({
    url: '/process-groups/' + pgId + '/template-instance',
    method: 'POST',
    json: {
      templateId: templateId,
      originX: 0,
      originY: 0
    }
  }, (err, res, body) => {
    if (err) {
      callback(err);
      return;
    }
    this.debug('instanciateTemplate', {statusCode: res.statusCode});
    switch (res.statusCode) {
      case 201:
        return callback(null, body.flow.processGroups[0].id);
      default:
        return callback(res);
    }
  });
}

var searchComponent = function(name, callback) {
  this.request('/flow/search-results?q=' + name, (err, res, body) => {
    if (err) {
      callback(err);
      return;
    }
    if (res.statusCode == 200) {
      var result = JSON.parse(body).searchResultsDTO;
      callback(null, result);
    } else {
      callback(res);
    }
  });
}

var updateProcessGroupState = function(uuid, running, callback) {
  this.getProcessGroupFlow(uuid, (err, pg) => {
    if (err) {
      callback(err);
      return;
    }
    this.putProcessGroupFlow(uuid, {
      id: uuid,
      state: running ? "RUNNING" : "STOPPED"
    }, (err) => {
      callback(err);
    })
  });
}

var moveProcessGroup = function(uuid, mover, callback) {
  this.getProcessGroup(uuid, (err, pg) => {
    if (err) {
      callback(err);
      return;
    }
    mover(pg.component.position);
    this.putProcessGroup(uuid, pg, (err) => {
      callback(err);
    })
  });
}

var updateProcessorState = function(uuid, running, callback) {
  this.getProcessor(uuid, (err, processor) => {
    if (err) {
      callback(err);
      return;
    }
    this.putProcessor(uuid, {
      revision: processor.revision,
      component: {
        id: uuid,
        state: running ? "RUNNING" : "STOPPED"
      }
    }, (err) => {
      callback(err);
    })
  });
}

var getComponent = function(path, callback) {
  this.request(path, (err, res, body) => {
    if (err) {
      callback(err);
      return;
    }
    this.debug('getComponent', {statusCode: res.statusCode});
    if (res.statusCode == 200) {
      callback(null, JSON.parse(body));
    } else {
      callback(res);
    }
  });
}

var getProcessor = function(uuid, callback) {
  this.getComponent('/processors/' + uuid, callback);
}

var getProcessGroup = function(uuid, callback) {
  this.getComponent('/process-groups/' + uuid, callback);
}

var getProcessGroupFlow = function(uuid, callback) {
  this.getComponent('/flow/process-groups/' + uuid, callback);
}


var postComponent = function(path, component, callback) {
  this.request({
    url: path,
    method: 'POST',
    json: component
  }, (err, res, body) => {
    if (err) {
      callback(err);
      return;
    }
    this.debug('postComponent', {statusCode: res.statusCode});
    if (res.statusCode == 201) {
      callback(null, res.headers.location);
    } else {
      callback(res);
    }
  });
}

var putComponent = function(path, component, callback) {
  this.request({
    url: path,
    method: 'PUT',
    json: component
  }, (err, res, body) => {
    if (err) {
      callback(err);
      return;
    }
    this.debug('putComponent', {statusCode: res.statusCode});
    if (res.statusCode == 200) {
      callback(null);
    } else {
      callback(res);
    }
  });
}

var putProcessGroup = function(uuid, pg, callback) {
  this.putComponent('/process-groups/' + uuid, pg, callback);
}

var putProcessGroupFlow = function(uuid, pg, callback) {
  this.putComponent('/flow/process-groups/' + uuid, pg, callback);
}

var putProcessor = function(uuid, processor, callback) {
  this.putComponent('/processors/' + uuid, processor, callback);
}

var putConnection = function(conn, callback) {
  this.putComponent('/connections/' + conn.component.id, conn, callback);
}

var deleteComponent = function(path, revision, callback) {
  var qs = {
    version: revision.version,
    clientId: revision.clientId
  };
  this.request({
    url: path,
    method: 'DELETE',
    qs: qs
  }, (err, res, body) => {
    if (err) {
      callback(err);
      return;
    }
    this.debug('deleteComponent', {statusCode: res.statusCode});
    if (res.statusCode == 200) {
      callback(null);
    } else {
      callback(res);
    }
  });
}

var findInputPortIdByName = function(portName, targetPgId, callback) {
  this.searchComponent(portName, (err, result) => {
    if (err) return callback(err);

    var targetPorts = result.inputPortResults.filter(port => {
      return targetPgId === port.groupId;
    });
    if (targetPorts.length !== 1) {
      return callback(new Error('Could not identify target inputport by name:' + portName));
    }

    return callback(null, targetPorts[0].id);
  });
}

var findOutputPortIdByName = function(portName, targetPgId, callback) {
  this.searchComponent(portName, (err, result) => {
    if (err) return callback(err);

    var targetPorts = result.outputPortResults.filter(port => {
      return targetPgId === port.groupId;
    });
    if (targetPorts.length !== 1) {
      return callback(new Error('Could not identify target outputport by name:' + portName));
    }

    return callback(null, targetPorts[0].id);
  });
}

var switchSourceConnection = function(conn, targetPgId, inputPortId, callback) {
  conn.component.destination.id = inputPortId;
  conn.component.destination.groupId = targetPgId;
  // Update connection.
  this.putConnection(conn, callback);
}

var createConnection = function(clientId, parentPgId, destinationProcessorId, targetPgId, outputPortId, callback) {
  /*
   * POST
   * http://localhost:8080/nifi-api/process-groups/b6a09099-0157-1000-9aa8-fcccef6172ac/connections
   *
   * {"revision":{"clientId":"bbcdbc47-0157-1000-f0aa-95644a368716","version":0},
   * "component":{"name":"","source":{"id":"b7d14854-0157-1000-a935-145406f161cf",
   * "groupId":"b7d14852-0157-1000-0285-d7ee38fb573a","type":"OUTPUT_PORT"},
   * "destination":{"id":"b7d17e1c-0157-1000-62ed-9a7afbaca64b",
   * "groupId":"b6a09099-0157-1000-9aa8-fcccef6172ac","type":"PROCESSOR"},
   * "flowFileExpiration":"0 sec","backPressureDataSizeThreshold":"1 GB",
   * "backPressureObjectThreshold":"10000","bends":[],"prioritizers":[]}}
   */

  var conn = {
    revision: {
      clientId: clientId,
      version: 0
    },
    component: {
      name: "",
      source: {
        id: outputPortId,
        groupId: targetPgId,
      },
      destination: {
        id: destinationProcessorId,
        groupId: parentPgId,
        type: "PROCESSOR"
      },
      flowFileExpiration: "0 sec",
      backPressureObjectThreshold: "10000",
      bends: [],
      prioritizers: []
    },
  };

  this.debug('update conn', conn);
  this.postComponent('/process-groups/' + parentPgId + '/connections', conn, callback);
}

exports.NiFiApi = NiFiApi;
exports.fromYamlFile = fromYamlFile;

