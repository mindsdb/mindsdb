group "default" {
  targets = ["bare", "devel", "cloud", "lightwood", "huggingface"]
}


variable "REGISTRY" {
  default = "454861456664.dkr.ecr.us-east-2.amazonaws.com"
}
variable "VERSION" {
  default = "unknown"
}


# This is effectively the base image for all of our images.
# We define it separately so we can use it as a base and only build it once.
target "builder" {
  dockerfile = "docker/mindsdb.Dockerfile"
  target = "build"
}
# Common traits of every image that we use to reduce duplication below.
target "_common" {
    dockerfile = "docker/mindsdb.Dockerfile" # If you change this, also change it in target:builder
    contexts = {
      builder = "target:builder" # Use a target to only perform base build steps once
    }
}



### IMAGES ###

target "bare" {
  inherits = ["_common"]
  tags = ["${REGISTRY}/mindsdb:${VERSION}", "${REGISTRY}/mindsdb:latest"]
}

target "devel" {
  inherits = ["_common"]
  tags = ["${REGISTRY}/mindsdb:${VERSION}-dev", "${REGISTRY}/mindsdb:dev"]
  target = "dev"
}

target "cloud" {
  inherits = ["_common"]
  args = {
    EXTRAS = ".[lightwood,huggingface,statsforecast_extra,neuralforecast_extra]"
  }
  tags = ["${REGISTRY}/mindsdb:${VERSION}-cloud", "${REGISTRY}/mindsdb:cloud"]
}

target "lightwood" {
  inherits = ["_common"]
  args = {
    EXTRAS = ".[lightwood]"
  }
  tags = ["${REGISTRY}/mindsdb:${VERSION}-lightwood", "${REGISTRY}/mindsdb:lightwood"]
}

target "huggingface" {
  inherits = ["_common"]
  args = {
    EXTRAS = ".[huggingface]"
  }
  tags = ["${REGISTRY}/mindsdb:${VERSION}-huggingface", "${REGISTRY}/mindsdb:huggingface"]
}