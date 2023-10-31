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

    def make_request(self, url, files={}):
        headers = {"x-api-key": self.api_key}
        resp = requests.post(url, headers=headers, files=files)
        content = {}
        if resp.status_code == 200:
            saved_path = self.write_to_file(resp.content)
            content = {'content': saved_path, 'code': resp.status_code}
        else:
            content = {'content': resp.text, 'code': resp.status_code}
        return content

    def image_extension_check(self, url):
        ext = url.split(".")[-1]
        if ext in ["jpeg", "jpg", "png"]:
            return ext
        raise Exception("Unknown image format. Currently jpg, jpeg & png are supported.")
    
    def download_image(self, url):
        img_ext = self.image_extension_check(url)
        res = requests.get(url)
        return {"img_ext": img_ext, "content": res.content}

    def remove_text(self, img_url):
        url = f'{self.base_endpoint}remove-text/v1'
        img_content = self.download_image(img_url)
        files = {
            'image_file': ('image.jpg', img_content["content"], f'image/{img_content["img_ext"]}')
        }
        return self.make_request(url, files=files)
