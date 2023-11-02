group "default" {
  targets = ["bare", "cloud"]
}

variable "TAG" {
  default = "latest"
}


variable "REGISTRY" {
  default = "454861456664.dkr.ecr.us-east-2.amazonaws.com"
}


target "_common" {
    dockerfile = "docker/mindsdb.Dockerfile"
    context = "."
}

target "bare" {
  inherits = ["_common"]
  tags = ["${REGISTRY}/mindsdb:${TAG}-bare"]
}

target "cloud" {
  inherits = ["_common"]
  args = {
    EXTRAS = ".[lightwood,huggingface]"
  }
  tags = ["${REGISTRY}/mindsdb:${TAG}-cloud"]
}
