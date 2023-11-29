group "default" {
  targets = ["builder", "bare", "cloud"]
}

variable "TAG" {
  default = "latest"
}


variable "REGISTRY" {
  default = "454861456664.dkr.ecr.us-east-2.amazonaws.com"
}


target "_common" {
    dockerfile = "docker/mindsdb.Dockerfile" # If you change this, also change it in target:builder
    contexts = {
      builder = "target:builder" # Use a target to only perform base build steps once
    }
}

target "builder" {
  dockerfile = "docker/mindsdb.Dockerfile"
  tags = ["${REGISTRY}/mindsdb:builder-${TAG}"]
}

target "bare" {
  inherits = ["_common"]
  tags = ["${REGISTRY}/mindsdb:bare-${TAG}"]
}

target "cloud" {
  inherits = ["_common"]
  args = {
    EXTRAS = ".[lightwood,huggingface,statsforecast_extra,neuralforecast_extra]"
  }
  tags = ["${REGISTRY}/mindsdb:cloud-${TAG}"]
}
