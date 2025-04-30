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
  default = "454861456664.dkr.ecr.us-east-2.amazonaws.com"
}
variable "PUSH_CACHE" {
  default = true
}
variable "CACHE_ONLY" {
  default = false
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

# Generate the list of tags for a given image.
# e.g. for the 'cloud' images this generates:
# - "mindsdb:cloud"        - This functions as a 'latest' tag for the cloud image
# - "mindsdb:v1.2.3-cloud" - For this specific version
# The same tags are pushed to dockerhub as well if the PUSH_TO_DOCKERHUB variable is set.
function "get_tags" {
  params = [image]
  result = [
    "${ECR_REPO}/${IMAGE}:${VERSION}${notequal(image, "bare") ? "-${image}" : ""}",
    "${ECR_REPO}/${IMAGE}:${notequal(image, "bare") ? image : "latest"}",
    PUSH_TO_DOCKERHUB ? "mindsdb/${IMAGE}:${VERSION}${notequal(image, "bare") ? "-${image}" : ""}" : "",
    PUSH_TO_DOCKERHUB ? "mindsdb/${IMAGE}:${notequal(image, "bare") ? image : "latest"}" : ""
  ]
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
        extras = ".[lightwood,huggingface,statsforecast-extra,neuralforecast-extra,timegpt,mssql,youtube,gmail,pgvector,writer,rag,github,snowflake,clickhouse,bigquery,elasticsearch,s3,dynamodb,databricks,oracle,teradata,hive,one_drive] darts datasetsforecast"
        target = ""
      },
      {
        name = "cloud-cpu"
        extras = ".[lightwood,huggingface_cpu,statsforecast-extra,neuralforecast-extra,timegpt,mssql,youtube,gmail,pgvector,writer,rag,github,snowflake,clickhouse,bigquery,elasticsearch,s3,dynamodb,databricks,oracle,teradata,hive,one_drive] darts datasetsforecast"
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

