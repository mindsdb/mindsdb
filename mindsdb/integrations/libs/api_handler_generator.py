import json
import yaml


class OpenAPISpecParser:
    def __init__(self, openapi_spec_path):
        with open(openapi_spec_path, 'r') as f:
            self.openapi_spec = json.loads(f.read()) if openapi_spec_path.endswith('.json') else yaml.safe_load(f)

    def get_security_schemes(self):
        return self.openapi_spec.get('components', {}).get('securitySchemes', {})