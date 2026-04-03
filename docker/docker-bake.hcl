# The default targets to be built if none are specified
group "default" {
  targets = ["bare", "devel", "cloud", "cloud-cpu", "lightwood", "huggingface", "huggingface-cpu"]
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
variable "PLATFORMS" {
  default = "linux/amd64,linux/arm64"
}
variable PLATFORM_LIST {
  default = split(",", PLATFORMS)
}
variable "BRANCH" {
  default = "main"
}
variable "ECR_REPO" {
  default = "168681354662.dkr.ecr.us-east-1.amazonaws.com"
}
variable "PUSH_CACHE" {
  default = true
}
variable "CACHE_ONLY" {
  default = false
}
variable "PRERELEASE" {
  default = can(regex("v?[0-9]+\\.[0-9]+\\.[0-9]+(a|b|rc)[0-9]+", VERSION))
}
variable "RELEASE_CANDIDATE" {
  default = can(regex("v?[0-9]+\\.[0-9]+\\.[0-9]+rc[0-9]+", VERSION))
}

function "get_cache_to" {
  params = [image]
  result = PUSH_CACHE ? [
    "type=registry,image-manifest=true,oci-mediatypes=true,mode=max,ref=${ECR_REPO}/${IMAGE}-cache:${replace("${BRANCH}", "/", "-")}-${image}"
  ] : []
}
function "get_cache_from" {
  params = [image]
  result = flatten([for p in PLATFORM_LIST:
    split("\n", <<EOT
type=registry,ref=${ECR_REPO}/${IMAGE}-cache:${replace("${BRANCH}", "/", "-")}-${image}
type=registry,ref=${ECR_REPO}/${IMAGE}-cache:main-${image}
EOT
    )
  ])
}

function "version_suffix" {
  params = [image]
  result = notequal(image, "bare") ? "-${image}" : ""
}
function "default_suffix" {
  params = [image]
  result = notequal(image, "bare") ? image : "latest"
}

# Generate the list of tags for a given image.
# e.g. for the 'cloud' images this generates:
# - "mindsdb:cloud"           - This functions as a 'latest' tag for the cloud image
# - "mindsdb:v1.2.3-cloud"    - For this specific version
# - "mindsdb:v1.2.3rc1-cloud" - For release candidates
# - "mindsdb:rc-cloud"        - Functions as a 'latest' for cloud release candidates
# - "mindsdb:rc-latest"       - Functions as a 'latest' for release candidates
# The same tags are pushed to dockerhub as well if the PUSH_TO_DOCKERHUB variable is set.
function "get_tags" {
  params = [image]
  result = compact([
    # ECR Tags
    "${ECR_REPO}/${IMAGE}:${VERSION}${version_suffix(image)}",
    RELEASE_CANDIDATE ? "${ECR_REPO}/${IMAGE}:rc-${default_suffix(image)}" : "${ECR_REPO}/${IMAGE}:${default_suffix(image)}",

    # Dockerhub Tags
    # Only release versions and release candidates are pushed to dockerhub
    PUSH_TO_DOCKERHUB && (!PRERELEASE || RELEASE_CANDIDATE) ? "mindsdb/${IMAGE}:${VERSION}${version_suffix(image)}" : "",
    PUSH_TO_DOCKERHUB && (!PRERELEASE || RELEASE_CANDIDATE) ? "mindsdb/${IMAGE}:rc-${default_suffix(image)}" : "",
    PUSH_TO_DOCKERHUB && !PRERELEASE ? "mindsdb/${IMAGE}:${default_suffix(image)}" : ""
  ])
} 



### OUTPUT IMAGES ###
target "base" {
  dockerfile = "docker/mindsdb.Dockerfile"
  platforms = PLATFORM_LIST
  target = "build"
  output = ["type=registry"]
}

target "images" {
  name = item.name
  dockerfile = "docker/mindsdb.Dockerfile"
  platforms = PLATFORM_LIST
  matrix = {
    item = [
      {
        name = "bare"
        extras = ""
        target = ""
      },
      {
        name = "devel"
        extras = ".[lightwood]"  # Required for running integration tests
        target = "dev"
      },
      {
        name = "lightwood"
        extras = ".[lightwood]"
        target = ""
      },
      {
        # If you make any changes here, make them to huggingface-cpu as well
        name = "huggingface"
        extras = ".[huggingface]"
        target = ""
      },
      {
        name = "huggingface-cpu"
        extras = ".[huggingface_cpu]"
        target = ""
      },
      {
        # If you make any changes here, make them to cloud-cpu as well
        name = "cloud"
        extras = ".[lightwood,huggingface,statsforecast-extra,neuralforecast-extra,timegpt,mssql,mssql-odbc,gmail,pgvector,rag,snowflake,clickhouse,bigquery,elasticsearch,s3,dynamodb,databricks,oracle,one_drive,opentelemetry,langfuse,jira,salesforce,gong,hubspot] darts datasetsforecast transformers"
        target = ""
      },
      {
        name = "cloud-cpu"
        extras = ".[lightwood,huggingface_cpu,statsforecast-extra,neuralforecast-extra,timegpt,mssql,mssql-odbc,gmail,pgvector,rag,snowflake,clickhouse,bigquery,elasticsearch,s3,dynamodb,databricks,oracle,one_drive,opentelemetry,langfuse,jira,salesforce,gong,hubspot] darts datasetsforecast transformers"
        target = ""
      },
    ]
  }
  target = item.target
  tags = get_tags(item.name)
  args = {
    EXTRAS = item.extras
  }
  cache-to = get_cache_to(item.name)
  cache-from = get_cache_from(item.name)
  contexts = {
    build = "target:base"
  }
  output = CACHE_ONLY ? ["type=cacheonly"] : ["type=registry"]
}

