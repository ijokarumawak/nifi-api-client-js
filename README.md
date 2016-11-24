# nifi-api-client-js
Javascript NiFi Web API client.

## How to install

```
# Clone this project
$ git clone https://github.com/ijokarumawak/nifi-api-client-js.git

# Install
$ cd nifi-api-client-js
$ npm install
```

## How to use

```js
// Specify a dir you downloaded the nifi-api-client-js project
var nifiApiClient = require('../nifi-api-client-js');

// Create a client instance from a Yaml config file.
var nifiApi = nifiApiClient.fromYamlFile('conf.yml', 'plain');

// Few examples:

// Find a process group id by name
nifiApi.findProcessGroupIdByName('ProcessGroup1', (err, result) => {
  if (err) return console.log('Failed to find a process group', err);
  console.log('got a process group', {result: result});
});

// Start a processor
var processorId = '93d40698-0158-1000-7b26-68c1a870aa94';
nifiApi.updateProcessorState(processorId, true, (err) => {
  if (err) return console.log('Failed to start a processor', err);
  console.log('Started a processor', processorId);
});
```
