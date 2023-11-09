from stability_sdk import client
from stability_sdk.client import generation
from PIL import Image
import io
import requests


class StabilityAPIClient:

    def _init_(self, api_key, dir_to_save, engine="stable-diffusion-xl-1024-v1-0", upscale_engine="esrgan-v1-x2plus"):
        """Initialize the stability wrapper api client.

        Args:
            api_key: Stability AI API Key
            dir_to_save: The local directory path to save the response images from the API
            engine (str, optional): Stability AI engine to use. Defaults to "stable-diffusion-xl-1024-v1-0".
            upscale_engine (str, optional): Stability AI upscaling engine to use. Defaults to "esrgan-v1-x2plus".

        Raises:
            ValueError: For unknown engine or upscale engine
        """
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
        """Save the binary image in the artifact to the local directory

        Args:
            artifact: Artifact returned by the API

        Returns:
            The local image path.
        """
        img = Image.open(io.BytesIO(artifact.binary))
        path = self.save_dir + str(artifact.seed) + ".png"
        img.save(path)
        return path

    def _process_artifacts(self, artifacts):
        """Process the artificats returned by the API

        Args:
            artifacts :Artifact returned by the API

        Returns:
            The saved image paths
        """
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
        """Get the existing engines

        Returns: Engines supported by the API
        """
        url = "https://api.stability.ai/v1/engines/list"
        if self.api_key is None:
            raise Exception("Missing Stability API key.")

        response = requests.get(url, headers={
            "Authorization": f"Bearer {self.api_key}"
        })

        if response.status_code != 200:
            raise Exception("Non-200 response: " + str(response.text))

        payload = response.json()

        return {engine["id"]: engine for engine in payload}

    def _is_valid_engine(self, engine_id):
        """Validates the given engine id against the supported engines by Stability

        Args:
            engine_id: The engine id to check

        Returns:
            True if valid engine else False
        """
        return engine_id in self.available_engines

    def _read_image_url(self, image_url):
        """Downloads the given image url.

        Args:
            image_url: The image url to download

        Returns:
            Downloaded image
        """
        return Image.open(requests.get(image_url, stream=True).raw)

    def text_to_image(self, prompt, height=1024, width=1024):
        """Converts the given text to image using stability API.

        Args:
            prompt: The given text
            height (int, optional): Height of the image. Defaults to 1024.
            width (int, optional): Width of the image. Defaults to 1024.

        Returns:
            The local saved paths of the generated images
        """
        answers = self.stability_api.generate(
            prompt=prompt,
            height=height,
            width=width
        )

        saved_images = self._process_artifacts(answers)

        return saved_images

    def image_to_image(self, image_url, prompt=None,
                       height=1024, width=1024,
                       mask_image_url=None):
        """Image to Image inpainting + masking

        Args:
            image_url: The image url
            prompt: The given text
            height (int, optional): Height of the image. Defaults to 1024.
            width (int, optional): Width of the image. Defaults to 1024.
            mask_image_url (string, optional): The mask image url for masking. Defaults to None.

        Returns:
            The local saved paths of the generated images
        """
        img = self._read_image_url(image_url)
        mask_img = None if (mask_image_url is None) else self._read_image_url(mask_image_url)
        if prompt is None:
            prompt = ""
        answers = self.stability_api.generate(prompt=prompt, init_image=img, mask_image=mask_img, width=width, height=height)

        saved_images = self._process_artifacts(answers)

        return saved_images

    def image_upscaling(self, image_url, height=None, width=None, prompt=None):
        """Image upscaling

        Args:
            image_url: The image url
            height (int, optional): Height of the image. Defaults to None.
            width (int, optional): Width of the image. Defaults to None.
            prompt: The given text

        Returns:
            The local saved paths of the generated images
        """
        img = self._read_image_url(image_url)
        if height is not None and width is not None:
            raise Exception("Either height or width can be given. Refer - https://platform.stability.ai/docs/features/image-upscaling#initial-generation-parameters")

        if height is None and width is None:
            answers = self.stability_api.upscale(init_image=img, prompt=prompt)

        if height is not None:
            answers = self.stability_api.upscale(init_image=img, height=height, prompt=prompt)

        if width is not None:
            answers = self.stability_api.upscale(init_image=img, width=width, prompt=prompt)

        saved_images = self._process_artifacts(answers)

        return saved_images
