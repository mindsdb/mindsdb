import requests
import uuid


class ClipdropClient:
    def __init__(self, api_key, local_dir):
        self.api_key = api_key
        self.dir_to_save = local_dir if local_dir.endswith("/") else local_dir + "/"
        self.base_endpoint = "https://clipdrop-api.co/"

    def write_to_file(self, img):
        f_name = self.dir_to_save + str(uuid.uuid4()) + ".png"
        with open(f_name, "wb") as f:
            f.write(img)
        return f_name

    def make_request(self, url, files={}, data={}):
        headers = {"x-api-key": self.api_key}
        resp = requests.post(url, headers=headers, files=files, data=data)
        if resp.status_code == 200:
            saved_path = self.write_to_file(resp.content)
            return saved_path
        return resp.text

    def image_extension_check(self, url):
        ext = url.split(".")[-1]
        if ext in ["jpeg", "jpg", "png"]:
            return ext
        raise Exception("Unknown image format. Currently jpg, jpeg & png are supported.")

    def download_image(self, path):

        img_ext = self.image_extension_check(path)
        try:
            res = requests.get(path)
            return {"img_ext": img_ext, "content": res.content}
        except Exception as e:
            raise Exception(f"Failed to download image: {e}")

    def remove_text(self, img_url):
        url = f'{self.base_endpoint}remove-text/v1'
        img_content = self.download_image(img_url)
        files = {
            'image_file': ('image.jpg', img_content["content"], f'image/{img_content["img_ext"]}')
        }
        return self.make_request(url, files=files)

    def remove_background(self, img_url):
        url = f'{self.base_endpoint}remove-background/v1'
        img_content = self.download_image(img_url)
        files = {
            'image_file': ('image.jpg', img_content["content"], f'image/{img_content["img_ext"]}')
        }
        return self.make_request(url, files=files)

    def sketch_to_image(self, img_url, prompt):
        url = f'{self.base_endpoint}sketch-to-image/v1/sketch-to-image'
        img_content = self.download_image(img_url)
        files = {
            'sketch_file': ('image.jpg', img_content["content"], f'image/{img_content["img_ext"]}')
        }
        data = {'prompt': prompt}
        return self.make_request(url, files=files, data=data)

    def text_to_image(self, prompt):
        url = f'{self.base_endpoint}text-to-image/v1'
        files = {
            'prompt': (None, prompt, 'text/plain')
        }
        return self.make_request(url, files=files)

    def replace_background(self, img_url, prompt):
        url = f'{self.base_endpoint}replace-background/v1'
        img_content = self.download_image(img_url)
        files = {
            'image_file': ('image.jpg', img_content["content"], f'image/{img_content["img_ext"]}')
        }
        data = {'prompt': prompt}
        return self.make_request(url, files=files, data=data)

    def reimagine(self, img_url):
        url = f'{self.base_endpoint}reimagine/v1/reimagine'
        img_content = self.download_image(img_url)
        files = {
            'image_file': ('image.jpg', img_content["content"], f'image/{img_content["img_ext"]}')
        }
        return self.make_request(url, files=files)
