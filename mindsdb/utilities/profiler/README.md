## Overview

The module allows you to analyze performance of individual query.

It stores in context structure: 
```
{
    'enabled': True,
    'tree': {
        'start_at': 123456,
        'stop_at': None,
        'name': 'root node',
        'children': [{
            'start_at': 234567,
            'stop_at': None,
            'name': 'child node',
            'children': []
        }],
    },
    'pointer': [1],
    'level': 0
}
```
 - enabled - is profiling enabled at the moment or not
 - tree - nested dict with tree nodes
 - pointer - list of integers which indicates index of node chiled on each level. Using that list is possible to get current node.
 - level - interer, indicates how deep in the tree we are at the moment. This value is changing even if `enabled is False`. It required because `enabled` may be cahnged at any moment. If `enabled is True` then we start to collect nodes only if `level == 1`.

Also initial profiling structure may be expanded with additional keys using `.set_meta` method.

Tree node structure:
```
{
    'start_at': timestamp,
    'stop_at': timestamp,
    'name': str,
    'children': [list of nodes]
}
```

To start/stop collect info need to do:
```
set profiling=true;
-- execute investigated queries
set profiling=false;
```

## API

There are 3 ways to use:

1. manually add start/stop in the code:
```
start('my tag')
function()
stop()
```
2. context:
```
with Context('my tag'):
    function()
```
3. decorator:
```
@profile('my tag')
def function():
    ...
```
