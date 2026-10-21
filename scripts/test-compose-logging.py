import argparse
import json
import subprocess
import time
import urllib.error
import urllib.request
import uuid

parser = argparse.ArgumentParser(
    description="Smoke-test built API/web images in disposable loopback-only containers."
)
parser.add_argument("--api-image", required=True)
parser.add_argument("--web-image", required=True)
args = parser.parse_args()
prefix = "logging-smoke-" + uuid.uuid4().hex[:10]
api = prefix + "-api"
web = prefix + "-web"
containers = []


def run(args):
    return subprocess.check_output(args, text=True).strip()


def status(url):
    try:
        with urllib.request.urlopen(url, timeout=2) as r:
            return r.status
    except urllib.error.HTTPError as r:
        return r.code


try:
    run(
        [
            "docker",
            "run",
            "-d",
            "--name",
            api,
            "-p",
            "127.0.0.1::26866",
            "-p",
            "127.0.0.1::80",
            "-e",
            "LOG_LEVEL=WARNING",
            "-e",
            "ACCESS_LOG_MODE=errors",
            "-e",
            "DATABASE_URI=sqlite:////home/cowork/.cowork/cowork.db",
            args.api_image,
        ]
    )
    containers.append(api)
    apiurl = "http://" + run(["docker", "port", api, "26866"])
    for _ in range(150):
        try:
            if status(apiurl + "/api/v1/health/") == 200:
                break
        except OSError:
            pass
        time.sleep(0.2)
    else:
        raise AssertionError("API readiness failed")
    # Run the health command baked into the image, not only a test-selected URL.
    health = json.loads(run(["docker", "inspect", api]))[0]["Config"]["Healthcheck"][
        "Test"
    ]
    assert health[0] == "CMD-SHELL"
    subprocess.run(["docker", "exec", api, "/bin/sh", "-c", health[1]], check=True)
    assert status(apiurl + "/not-a-route?token=private-fixture") == 401
    run(
        [
            "docker",
            "run",
            "-d",
            "--name",
            web,
            "--network",
            "container:" + api,
            "-e",
            "ACCESS_LOG_MODE=all",
            args.web_image,
        ]
    )
    containers.append(web)
    url = "http://" + run(["docker", "port", api, "80"])
    for _ in range(100):
        try:
            if status(url + "/healthz?token=private-fixture") == 200:
                break
        except OSError:
            pass
        time.sleep(0.1)
    else:
        raise AssertionError("web readiness failed")
    assert status(url + "/") == 200
    with urllib.request.urlopen(url + "/api/v1/health/", timeout=3) as response:
        assert response.status == 200
        assert response.headers.get_content_type() == "application/json"
        assert isinstance(json.load(response), dict)
    subprocess.run(
        [
            "docker",
            "exec",
            web,
            "mv",
            "/usr/share/nginx/html/index.html",
            "/tmp/index.html",
        ],
        check=True,
    )
    assert status(url + "/healthz?token=private-fixture") == 503
    versions = json.loads(
        run(
            [
                "docker",
                "exec",
                api,
                "python",
                "-c",
                'import importlib.metadata as m,json; print(json.dumps({x:m.version(x) for x in ["anton-agent","cowork-server","structlog"]}))',
            ]
        )
    )
    results = {"versions": versions}
    for name in reversed(containers):
        subprocess.run(
            ["docker", "stop", "--time", "5", name],
            check=True,
            stdout=subprocess.DEVNULL,
        )
        logs = subprocess.check_output(
            ["docker", "logs", name], stderr=subprocess.STDOUT, text=True
        )
        assert "private-fixture" not in logs
        events = [json.loads(x) for x in logs.splitlines() if x.startswith("{")]
        if name == web:
            codes = [
                x["status_code"] for x in events if x.get("logger") == "nginx.access"
            ]
            assert codes == [200, 200, 503], codes
            results["web_access"] = codes
        else:
            assert any(x.get("status_code") == 401 for x in events), events
            assert not any(
                x.get("event") == "http_access" and x.get("status_code") == 200
                for x in events
            )
            results["api_failed_request_visible"] = True
    print(json.dumps(results, indent=2))
finally:
    for name in reversed(containers):
        subprocess.run(
            ["docker", "rm", "-f", name], check=True, stdout=subprocess.DEVNULL
        )
