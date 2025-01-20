from mindsdb.integrations.libs.settings import IntegrationSettings

class UpstashVectorSettings(IntegrationSettings):
    name = "upstash_vector"
    display_name = "Upstash Vector"
    handler_path = "upstash_handler.UpstashVectorHandler"
    icon_path = "icon.png"
