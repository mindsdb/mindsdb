# The default targets to be built if none are specified
group "default" {
  targets = ["bare", "devel", "cloud", "lightwood", "huggingface"]
}

variable "PUSH_TO_DOCKERHUB" {
  default = false
}
variable "IMAGE" {
  default = "mindsdb"
}
# This is a semver for releases but otherwise is a github sha
variable "VERSION" {
  default = "unknown"
}

# Generate the list of tags for a given image.
# e.g. for the 'cloud' images this generates:
# - "mindsdb:cloud"        - This functions as a 'latest' tag for the cloud image
# - "mindsdb:v1.2.3-cloud" - For this specific version
# The same tags are pushed to dockerhub as well if the PUSH_TO_DOCKERHUB variable is set.
function "get_tags" {
  params = [target]
  result = [
    "454861456664.dkr.ecr.us-east-2.amazonaws.com/${IMAGE}:${VERSION}${notequal(target, "") ? "-" : ""}${target}",
    "454861456664.dkr.ecr.us-east-2.amazonaws.com/${IMAGE}:${notequal(target, "") ? target : "latest"}",
    PUSH_TO_DOCKERHUB ? "mindsdb/${IMAGE}:${VERSION}${notequal(target, "") ? "-" : ""}${target}" : "",
    PUSH_TO_DOCKERHUB ? "mindsdb/${IMAGE}:${notequal(target, "") ? target : "latest"}" : ""
  ]
} 

# This is effectively the base image for all of our images.
# We define it separately so we can use it as a base and only build it once.
target "builder" {
  dockerfile = "docker/mindsdb.Dockerfile"
  target = "build"
  platforms = ["linux/amd64", "linux/arm64"]
}

# Common traits of every image that we use to reduce duplication below.
target "_common" {
  dockerfile = "docker/mindsdb.Dockerfile" # If you change this, also change it in target:builder
  contexts = {
    build = "target:builder" # Use a target to only perform base build steps once
  }
  platforms = ["linux/amd64", "linux/arm64"]
}



### OUTPUT IMAGES ###

target "bare" {
  inherits = ["_common"]
  tags = get_tags("")
}

target "devel" {
  inherits = ["_common"]
  tags = get_tags("dev")
  target = "dev"
}

target "cloud" {
  inherits = ["_common"]
  tags = get_tags("cloud")
  args = {
    EXTRAS = ".[lightwood,huggingface,statsforecast-extra,neuralforecast-extra,timegpt,surrealdb,mssql,youtube,ignite,gmail,pgvector,llama_index,writer,rag,github,snowflake,clickhouse] darts datasetsforecast"
  }
}

target "lightwood" {
  inherits = ["_common"]
  tags = get_tags("lightwood")
  args = {
    EXTRAS = ".[lightwood]"
  }
}

target "huggingface" {
  inherits = ["_common"]
  tags = get_tags("huggingface")
  args = {
    EXTRAS = ".[huggingface]"
  }
}