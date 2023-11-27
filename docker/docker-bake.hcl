group "default" {
  targets = ["builder", "bare-ecr", "cloud-ecr"]
}
group "release" {
  targets = ["bare", "devel", "lightwood", "huggingface"]
}

variable "TAG" {
  default = "latest"
}


variable "ECR_REGISTRY" {
  default = "454861456664.dkr.ecr.us-east-2.amazonaws.com"
}

variable "VERSION" {
  default = "unknown"
}

target "_common" {
    dockerfile = "docker/mindsdb.Dockerfile" # If you change this, also change it in target:builder
    contexts = {
      builder = "target:builder" # Use a target to only perform base build steps once
    }
}

target "builder" {
  dockerfile = "docker/mindsdb.Dockerfile"
  target = "build"
}

target "bare-ecr" {
  inherits = ["_common"]
  tags = ["${ECR_REGISTRY}/mindsdb:bare-${TAG}"]
}

target "cloud-ecr" {
  inherits = ["_common"]
  args = {
    EXTRAS = ".[lightwood,huggingface,statsforecast_extra,neuralforecast_extra]"
  }
  tags = ["${ECR_REGISTRY}/mindsdb:cloud-${TAG}"]
}



### DOCKERHUB IMAGES ###

target "bare" {
  inherits = ["_common"]
  tags = ["mindsdb/mindsdb:${VERSION}", "mindsdb/mindsdb:latest"]
}

target "devel" {
  inherits = ["_common"]
  tags = ["mindsdb/mindsdb:${VERSION}-dev", "mindsdb/mindsdb:dev"]
  target = "dev"
}

target "lightwood" {
  inherits = ["_common"]
  args = {
    EXTRAS = ".[lightwood]"
  }
  tags = ["mindsdb/mindsdb:${VERSION}-lightwood", "mindsdb/mindsdb:lightwood"]
}

target "huggingface" {
  inherits = ["_common"]
  args = {
    EXTRAS = ".[huggingface]"
  }
  tags = ["mindsdb/mindsdb:${VERSION}-huggingface", "mindsdb/mindsdb:huggingface"]
}