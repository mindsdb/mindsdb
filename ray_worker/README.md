
Problems and known solutions
- environment variables fixed in service script. Need to use AWS secret manager
- anyscale service gets new url and token on every deploy. Maybe is bettery to use load balancer: after deploy new version and pre-heat it switch load balancer to new version.
- no direct access to database. Now it is done using ssh tunnel. Need to allow access in AWS or run anyscale in same zone.
- wasn't be able to get to worker node, need to find ssh key

How to run locally:
- serve run ray_worker.ray_server:entrypoint

How to deploy service
- create cluster image with selected version of mindsdb:
  - Create mindsdb_cluster.yaml file (as in example)
  - Then execute: `anyscale cluster-env build mindsdb_cluster.yaml --name mindsdb-ray-env`
- create worker
  - set-up parameters to database and S3 in ray_server.py
  - zip in archive
  - put it in web server (accessible via https)
  - create service.yaml with path to this file (as in example)
  - deploy it by running `anyscale service deploy service.yaml'
