# mindsdb

[MindsDB](https://mindsdb.com?utm_medium=community&utm_source=github&utm_campaign=mindsdb%20repo) enables you to use ML predictions in your database using SQL.

- Developers can quickly add AI capabilities to your applications.
- Data Scientists can streamline MLOps by deploying ML models as AI Tables.
- Data Analysts can easily make forecasts on complex data (like multivariate time-series with high cardinality) and visualize them in BI tools like Tableau.

# Pre-requisites

- Kubernetes
- Helm 3.0+

# Installing the Chart

```bash
helm upgrade -i \
  mindsdb mindsdb \
  --namespace mindsdb \
  --create-namespace
```

# Configuration

All the configurations can be done in the [values.yaml](./mindsdb/values.yaml) file or you can create a separate YAML file with only the values that you want to override and pass it with a `-f` to the `helm install` command

### Image

| Parameter          | Description                                                                             | Default           |
| ------------------ | --------------------------------------------------------------------------------------- | ----------------- |
| `image.repository` | Image to start for this pod                                                             | `mindsdb/mindsbd` |
| `image.tag`        | [Image tag](https://hub.docker.com/r/mindsdb/mindsdb/tags?page=1&ordering=last_updated) | `latest`          |
| `image.pullPolicy` | Image pull policy                                                                       | `Always`          |

### Ingress

| Parameter                            | Description                                                                 | Default                                            |
| ------------------------------------ | --------------------------------------------------------------------------- | -------------------------------------------------- |
| `ingress.enabled`                    | enable ingress                                                              | `false`                                            |
| `ingress.annotations`                | add ingress annotations                                                     |                                                    |
| `ingress.hosts[0].host`              | add hosts for ingress                                                       | `chart-example.local`                              |
| `ingress.hosts[0].paths[0].path`     | add path for each ingress host                                              | `/`                                                |
| `ingress.hosts[0].paths[0].pathType` | add ingress path type                                                       | `ImplementationSpecific`                           |
| `ingress.tls`                        | add ingress tls settings                                                    | `[]`                                               |
| `ingress.className`                  | add ingress class name. Only used in k8s 1.19+                              |                                                    |
| `ingress.apiVersion`                 | specify APIVersion of ingress object. Mostly would only be used for argocd. | version indicated by helm's `Capabilities` object. |

### Service

#### Web

| Parameter                               | Description                                                                                                  | Default     |
| --------------------------------------- | ------------------------------------------------------------------------------------------------------------ | ----------- |
| `service.http.enabled`                  | Enable Web service                                                                                           | `true`      |
| `service.http.type`                     | Kubernetes service type for web traffic                                                                      | `ClusterIP` |
| `service.http.port`                     | Port for web traffic                                                                                         | `47334`     |
| `service.http.clusterIP`                | ClusterIP setting for http autosetup for statefulset is None                                                 | `None`      |
| `service.http.loadBalancerIP`           | LoadBalancer Ip setting                                                                                      |             |
| `service.http.nodePort`                 | NodePort for http service                                                                                    |             |
| `service.http.externalTrafficPolicy`    | If `service.http.type` is `NodePort` or `LoadBalancer`, set this to `Local` to enable source IP preservation |             |
| `service.http.externalIPs`              | http service external IP addresses                                                                           |             |
| `service.http.loadBalancerSourceRanges` | Source range filter for http loadbalancer                                                                    | `[]`        |
| `service.http.annotations`              | http service annotations                                                                                     |             |

#### MySQL

| Parameter                               | Description                                                                                                  | Default     |
| --------------------------------------- | ------------------------------------------------------------------------------------------------------------ | ----------- |
| `service.mysql.enabled`                  | Enable MySQL service                                                                                           | `true`      |
| `service.mysql.type`                     | Kubernetes service type for MySQL traffic                                                                      | `ClusterIP` |
| `service.mysql.port`                     | Port for MySQL traffic                                                                                         | `47335`     |
| `service.mysql.clusterIP`                | ClusterIP setting for MySQL autosetup for statefulset is None                                                 | `None`      |
| `service.mysql.loadBalancerIP`           | LoadBalancer Ip setting                                                                                      |             |
| `service.mysql.nodePort`                 | NodePort for http service                                                                                    |             |
| `service.mysql.externalTrafficPolicy`    | If `service.mysql.type` is `NodePort` or `LoadBalancer`, set this to `Local` to enable source IP preservation |             |
| `service.mysql.externalIPs`              | MySQL service external IP addresses                                                                           |             |
| `service.mysql.loadBalancerSourceRanges` | Source range filter for MySQL loadbalancer                                                                    | `[]`        |
| `service.mysql.annotations`              | MySQL service annotations                                                                                     |             |

#### MongoDB

| Parameter                               | Description                                                                                                  | Default     |
| --------------------------------------- | ------------------------------------------------------------------------------------------------------------ | ----------- |
| `service.mongodb.enabled`                  | Enable MongoDB service                                                                                           | `true`      |
| `service.mongodb.type`                     | Kubernetes service type for MongoDB traffic                                                                      | `ClusterIP` |
| `service.mongodb.port`                     | Port for MongoDB traffic                                                                                         | `47336`     |
| `service.mongodb.clusterIP`                | ClusterIP setting for MongoDB autosetup for statefulset is None                                                 | `None`      |
| `service.mongodb.loadBalancerIP`           | LoadBalancer Ip setting                                                                                      |             |
| `service.mongodb.nodePort`                 | NodePort for http service                                                                                    |             |
| `service.mongodb.externalTrafficPolicy`    | If `service.mongodb.type` is `NodePort` or `LoadBalancer`, set this to `Local` to enable source IP preservation |             |
| `service.mongodb.externalIPs`              | MongoDB service external IP addresses                                                                           |             |
| `service.mongodb.loadBalancerSourceRanges` | Source range filter for MongoDB loadbalancer                                                                    | `[]`        |
| `service.mongodb.annotations`              | MongoDB service annotations                                                                                     |             |
