# mindsdb

[MindsDB](https://mindsdb.com?utm_medium=community&utm_source=github&utm_campaign=mindsdb%20repo) enables you to use ML predictions in your database using SQL.

- Developers can quickly add AI capabilities to your applications.
- Data Scientists can streamline MLOps by deploying ML models as AI Tables.
- Data Analysts can easily make forecasts on complex data (like multivariate time-series with high cardinality) and visualize them in BI tools like Tableau.

# Pre-requisites

- Kubernetes
- Microk8s
- NFS
- ssl
- storageclass

# Installing Mindsdb on kubernetes


# Steps which you need to follow before the deployment :-

Download all the files
Configure the files as you need 
create path-to-mount/mindsdb_config.json  (copy the file which is inside the repo one can configure accordingly)


# How to deploy the 

```bash
## Change your directory outside the configured manifest files folder and than deploy it on kubernetes through this command  
kubectl create -f mindsdb/
## or you can deploy each file one by one
kubectl create -f pv.yaml/
kubectl create -f pvc.yaml/
kubectl create -f ingress.yaml/
kubectl create -f http-svc.yaml/
kubectl create -f mongodb-svc.yaml/
kubectl create -f mysql-svc.yaml/
kubectl create -f mindsdb-deployment.yaml/

```


### mindsdbdeployment.yaml

| Parameter          | Description                                                                             | Default           |
| ------------------ | --------------------------------------------------------------------------------------- | ----------------- |
| `image`            | Image to start for this pod                                                             | `mindsdb/mindsbd` |
| `image-tag`        | [Image tag](https://hub.docker.com/r/mindsdb/mindsdb/tags?page=1&ordering=last_updated) | `latest`          |
| `imagepullPolicy`  | Image pull policy                                                                       | `IfNotPresent`    |
| `volumeMounts`     | volumesmount is used for storing the mindsdb_config.json                                | `mindsdb-config`  |
| `resources`        | adding the resources which should be used my the pod you can also add limits            |                   |
| `volumes`          | connection of the volumeMounts                                                          | `mindsdb-config`  |
| `resources.memory` | Add ur required memory                                                                  | `500Mi`           |
| `resources.cpu`    | Add ur required cpu                                                                     | `100m`            |
| `claimname`        | Add the generated pvc name                                                              | `mindsdb-data-pvc`|



### ingress.yaml

| Parameter                            | Description                                                                 | Default                                            |
| ------------------------------------ | --------------------------------------------------------------------------- | -------------------------------------------------- |
| `annotations`                        | add ingress annotations                                                     |                                                    |
| `host`                               | add hosts for ingress                                                       | `chart-example.local`                              |
| `path`                               | add path for each ingress host                                              | `/`                                                |
| `pathType`                           | add ingress path type                                                       | `ImplementationSpecific`                           |
| `tls`                                | add ingress tls settings                                                    | `your-ssl`                                         |
| `className`                          | add ingress class name. Only used in k8s 1.19+                              |                                                    |
| `apiVersion`                         | specify APIVersion of ingress object. Mostly would only be used for argocd. | `add the desired version.`                         |

### Service

#### http-svc.yaml

| Parameter                               | Description                                                                                                  | Default     |
| --------------------------------------- | ------------------------------------------------------------------------------------------------------------ | ----------- |
| `type`                                  | Kubernetes service type for web traffic (`ClusterIP,NodePort,Loadbalancer`)                                  | `NodePort` |
| `port`                                  | Port for web traffic                                                                                         | `47334`     |
| `clusterIP`                             | ClusterIP setting for http autosetup for statefulset is None                                                 | `None`      |
| `loadBalancerIP`                        | LoadBalancer Ip setting                                                                                      |             |
| `nodePort`                              | NodePort for http service                                                                                    | `30081`     |
| `annotations`                           | http service annotations                                                                                     |             |

#### mysql-svc.yaml

| Parameter                               | Description                                                                                                  | Default     |
| --------------------------------------- | ------------------------------------------------------------------------------------------------------------ | ----------- |
| `type`                     | Kubernetes service type for MySQL traffic (`ClusterIP,NodePort,Loadbalancer`)                                             | `ClusterIP` |
| `port`                     | Port for MySQL traffic                                                                                                    | `47335`     |
| `clusterIP`                | ClusterIP setting for MySQL autosetup for statefulset is None                                                             | `None`      |
| `loadBalancerIP`           | LoadBalancer Ip setting                                                                                                   |             |
| `nodePort`                 | NodePort for http service                                                                                                 |             |
| `annotations`              | MySQL service annotations                                                                                                 |             |

#### mongodb-svc.yaml

| Parameter                               | Description                                                                                                  | Default     |
| --------------------------------------- | ------------------------------------------------------------------------------------------------------------ | ----------- |
| `type`                     | Kubernetes service type for MongoDB traffic (`ClusterIP,NodePort,Loadbalancer`)                                           | `ClusterIP` |
| `port`                     | Port for MongoDB traffic                                                                                                  | `47336`     |
| `clusterIP`                | ClusterIP setting for MongoDB autosetup for statefulset is None                                                           | `None`      |
| `loadBalancerIP`           | LoadBalancer Ip setting                                                                                                   |             |
| `nodePort`                 | NodePort for http service                                                                                                 |             |
| `annotations`              | MongoDB service annotations                                                                                               |             |


#### pv.yaml

| Parameter                               | Description                                                                                                  | Default                   |
| --------------------------------------- | ------------------------------------------------------------------------------------------------------------ | ------------------------- |
| `storageclass`                          | Enter the storageclass which is connected to the kubernetes instance                                         | `your-storageclass`       |
| `path`                                  | Add path of the nfs mount folder                                                                             | `47336`                   |
| `apiVersion`                            | specify APIVersion of ingress object. Mostly would only be used for argocd.                                  | `add the desired version.`|


#### pvc.yaml

| Parameter                               | Description                                                                                                  | Default                   |
| --------------------------------------- | ------------------------------------------------------------------------------------------------------------ | ------------------------- |
| `storageclass`                          | Enter the storageclass which is connected to the kubernetes instance                                         | `your-storageclass`       |
| `apiVersion`                            | specify APIVersion of ingress object. Mostly would only be used for argocd.                                  | `add the desired version.`|


#### mindsdb-deployment.yaml

| Parameter                               | Description                                                                                                  | Default                   |
| --------------------------------------- | ------------------------------------------------------------------------------------------------------------ | ------------------------- |
| `storageclass`                          | Enter the storageclass which is connected to the kubernetes instance                                         | `ClusterIP`               |
| `apiVersion`                            | specify APIVersion of ingress object. Mostly would only be used for argocd.                                  | `add the desired version.`|