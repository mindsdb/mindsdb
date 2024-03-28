# The default targets to be built if none are specified
group "default" {
  targets = ["bare", "devel", "cloud", "cloud-data", "lightwood", "huggingface"]
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
variable "PLATFORM" {
  default = "linux/amd64"
}
variable "BRANCH" {
  default = "stable"
}

function "get_platform_tag" {
  params = []
  result = replace("${equal(PLATFORM, "") ? "" : "-"}${PLATFORM}", "linux/", "")
}

function "get_cache_to" {
  params = [target]
  result = [
    "type=registry,image-manifest=true,oci-mediatypes=true,mode=max,ref=454861456664.dkr.ecr.us-east-2.amazonaws.com/${IMAGE}-cache:${replace("${BRANCH}", "/", "-")}-${target}${get_platform_tag()}"
  ]
}
function "get_cache_from" {
  params = [target]
  result = [
    "type=registry,ref=454861456664.dkr.ecr.us-east-2.amazonaws.com/${IMAGE}-cache:${replace("${BRANCH}", "/", "-")}-${target}${get_platform_tag()}",
    "type=registry,ref=454861456664.dkr.ecr.us-east-2.amazonaws.com/${IMAGE}-cache:staging-${target}${get_platform_tag()}",
    "type=registry,ref=454861456664.dkr.ecr.us-east-2.amazonaws.com/${IMAGE}-cache:stable-${target}${get_platform_tag()}"
  ]
}

# Generate the list of tags for a given image.
# e.g. for the 'cloud' images this generates:
# - "mindsdb:cloud"        - This functions as a 'latest' tag for the cloud image
# - "mindsdb:v1.2.3-cloud" - For this specific version
# The same tags are pushed to dockerhub as well if the PUSH_TO_DOCKERHUB variable is set.
function "get_tags" {
  params = [target]
  result = [
    "454861456664.dkr.ecr.us-east-2.amazonaws.com/${IMAGE}:${VERSION}${notequal(target, "bare") ? "-${target}" : ""}${get_platform_tag()}",
    "454861456664.dkr.ecr.us-east-2.amazonaws.com/${IMAGE}:${notequal(target, "bare") ? target : "latest"}${get_platform_tag()}",
    PUSH_TO_DOCKERHUB ? "mindsdb/${IMAGE}:${VERSION}${notequal(target, "bare") ? "-${target}" : ""}${get_platform_tag()}" : "",
    PUSH_TO_DOCKERHUB ? "mindsdb/${IMAGE}:${notequal(target, "bare") ? target : "latest"}${get_platform_tag()}" : ""
  ]
} 



### OUTPUT IMAGES ###

target "images" {
  name = item.name
  dockerfile = "docker/mindsdb.Dockerfile" # If you change this, also change it in target:builder
  platforms = ["${PLATFORM}"]
  matrix = {
    item = [
      {
        name = "bare"
        extras = ""
        target = ""
        base_image = null
      },
      {
        name = "devel"
        extras = ""
        target = "dev"
        base_image = null
      },
      {
        name = "lightwood"
        extras = ".[lightwood]"
        target = ""
        base_image = "nvidia/cuda:12.2.0-runtime-ubuntu22.04"
      },
      {
        name = "huggingface"
        extras = ".[huggingface]"
        target = ""
        base_image = "nvidia/cuda:12.2.0-runtime-ubuntu22.04"
      },
      {
        name = "cloud"
        extras = ".[lightwood,huggingface,statsforecast-extra,neuralforecast-extra,timegpt,surrealdb,mssql,youtube,ignite,gmail,pgvector,llama_index,writer,rag,github,snowflake,clickhouse,couchbase,twelve_labs] darts datasetsforecast"
        target = ""
        base_image = "nvidia/cuda:12.2.0-runtime-ubuntu22.04"
      },
      {
        name = "cloud-data"
        extras = ".[surrealdb,mssql,youtube,ignite,gmail,pgvector,github,snowflake,clickhouse,couchbase]"
        target = ""
        base_image = null
      },
    ]
  }
  target = item.target
  tags = get_tags(item.name)
  args = {
    EXTRAS = item.extras,
    BASE_IMAGE = item.base_image
  }
  cache-to = get_cache_to(item.name)
  cache-from = get_cache_from(item.name)
}

