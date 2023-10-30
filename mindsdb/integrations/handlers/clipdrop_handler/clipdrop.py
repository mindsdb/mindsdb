import requests
import uuid


class ClipdropClient:
    def __init__(self, api_key, local_dir):
        self.api_key = api_key
        self.dir_to_save = local_dir if local_dir.endswith("/") else local_dir + "/"
        self.base_endpoint = "https://clipdrop-api.co/"
        
    def write_to_file(self, img):
        f_name = self.dir_to_save + str(uuid.uuid4())
        with open(f_name, "wb") as f:
            f.write(img)
        return f_name

    def make_request(self, url, files={}):
        headers = {"x-api-key": self.api_key}
        resp = requests.get(url, headers=headers, files=files)
        content = {}
        if resp.status_code == 200:
            saved_path = self.write_to_file(resp.content)
            content = {'content': saved_path, 'code': resp.status_code}
        else:
            content = {'content': resp.text, 'code': resp.status_code}
        return content

    def image_extension_check(self, url):
        return url.endswith("jpeg") or url.endswith("jpg") or url.endswith("png")
    
    def download_image(self, url):
        if self.image_extension_check(url):
            res = requests.get(url)
            return res.content
        raise Exception("Unknown image format. Currently jpg, jpeg & png are supported.")

    def remove_background(self, img_url):
        url = f'{self.base_endpoint}remove-text/v1'
        img_content = self.download_image(img_url)
        files = {
            'image_file': ('image.jpg', img_content, 'image/jpeg')
        }
        return self.make_request(url, files=files)
