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

function "get_cache_to" {
  params = [image]
  result = length(PLATFORM_LIST) > 1 ? [] : [
    "type=registry,image-manifest=true,oci-mediatypes=true,mode=max,ref=${ECR_REPO}/${IMAGE}-cache:${replace("${BRANCH}", "/", "-")}-${image}-${replace("${PLATFORM_LIST[0]}", "linux/", "")}"
  ]
}
function "get_cache_from" {
  params = [image]
  result = flatten([for p in PLATFORM_LIST:
    split("\n", <<EOT
type=registry,ref=${ECR_REPO}/${IMAGE}-cache:${replace("${BRANCH}", "/", "-")}-${image}-${replace("${p}", "linux/", "")}
type=registry,ref=${ECR_REPO}/${IMAGE}-cache:main-${image}-${replace("${p}", "linux/", "")}
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
        extras = ""
        target = "dev"
      },
      {
        name = "lightwood"
        extras = ".[lightwood]"
        target = ""
      },
      {
        name = "huggingface"
        extras = ".[huggingface]"
        target = ""
      },
      {
        name = "cloud"
        extras = ".[lightwood,huggingface,statsforecast-extra,neuralforecast-extra,timegpt,mssql,youtube,gmail,pgvector,writer,rag,github,snowflake,clickhouse,bigquery,elasticsearch,s3,dynamodb,databricks,oracle,teradata,hive] darts datasetsforecast"
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
}

