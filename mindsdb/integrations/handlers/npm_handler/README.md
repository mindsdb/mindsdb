# NPM Handler

This handler allows you to interact with the data that [npms.io](https://npms.io) stores stats about various NPM packages.

## About NPM

The free npm Registry has become the center of JavaScript code sharing, and with more than two million packages, the largest software registry in the world.

## NPM Handler Implementation

This implementation is based on the API service provided at [api.npms.io](https://api.npms.io/).

## NPM Handler Initialization

There is nothing needed to be passed in the database initialization process. You can create the database via the following flow.

```sql
CREATE DATABASE npm_datasource
WITH ENGINE = 'npm';
```

To select from various tables, you can use `SELECT` statement. You must provide a package for this to work.

```sql
SELECT * FROM npm_datasource.dependencies
WHERE package="handlebars";
```

```sql
SELECT username FROM npm_datasource.maintainers
WHERE package="handlebars";
```

Note that some of the stats can be slightly outdated.

## Available tables

- `metadata`: things like name, description, license, etc.
- `maintainers`: list of maintainers and their emails
- `keywords`: keywords associated with the package
- `dependencies`: dependencies of the package
- `dev_dependencies`: development dependencies of the package
- `optional_dependencies`: optional dependencies of the package
- `github_stats`: some github stats like number of stars, forks etc.
