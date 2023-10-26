from stability_sdk import client
from stability_sdk.client import generation
from PIL import Image
import io, requests

class StabilityAPIClient:
    
    def __init__(self, api_key, dir_to_save, 
                 engine="stable-diffusion-xl-1024-v1-0", upscale_engine="esrgan-v1-x2plus"):
        self.api_key = api_key
        self.STABILITY_HOST = 'grpc.stability.ai:443'
        self.save_dir = dir_to_save + "/" if not dir_to_save.endswith("/") else dir_to_save
        self.available_engines = self.get_existing_engines()
        if not self._is_valid_engine(engine):
            raise ValueError("Unknown engine. The available engines are - " + self.available_engines)
        if not self._is_valid_engine(upscale_engine):
            raise ValueError("Unknown upscale engine. The available engines are - " + self.available_engines)
        
        self.stability_api = client.StabilityInference(host=self.STABILITY_HOST, 
                                                       key=self.api_key,
                                                       engine=engine,
                                                       upscale_engine=upscale_engine,
                                                       verbose=True)
        
    def save_image(self, artifact):
        img = Image.open(io.BytesIO(artifact.binary))
        path = self.save_dir + str(artifact.seed)+ ".png"
        img.save(path)
        return path
    
    def _process_artifacts(self, artifacts):
        saved_image_paths = []
        for resp in artifacts:
            for artifact in resp.artifacts:
                if artifact.finish_reason == generation.FILTER:
                    saved_image_paths.append(
                        "Your request activated the API's safety filters \
                        and could not be processed. Please modify the prompt and try again.")
                if artifact.type == generation.ARTIFACT_IMAGE:
                    saved_image_paths.append(self.save_image(artifact))
        return saved_image_paths
    
    def get_existing_engines(self):
        url = "https://api.stability.ai/v1/engines/list"
        if self.api_key is None: raise Exception("Missing Stability API key.")

        response = requests.get(url, headers={
            "Authorization": f"Bearer {self.api_key}"
        })

        if response.status_code != 200:
            raise Exception("Non-200 response: " + str(response.text))

        payload = response.json()
        
        return {engine["id"]:engine for engine in payload}
    
    def _is_valid_engine(self, engine_id):
        return engine_id in self.available_engines
    
    def _read_image_url(self, image_url):
        return Image.open(requests.get(image_url, stream=True).raw)
        
    def text_to_image(self, prompt, seed=121245125, 
                      height=1024, width=1024, 
                      steps=50):
        
        answers = self.stability_api.generate(
            prompt=prompt,
            seed=seed,
            height=height,
            width=width,
            steps=steps,
            sampler=generation.SAMPLER_K_DPMPP_2M
        )
        
        saved_images = self._process_artifacts(answers)
        
        return saved_images
