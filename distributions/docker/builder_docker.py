about = {}
with open("mindsdb/__about__.py") as fp:
    exec(fp.read(), about)
version = about['__version__']

with open('distribtuions/docker/Dockerfile', 'r') as fp:
    content = fp.read()
    content.replace('$version', version)

with open('distribtuions/docker/Dockerfile', 'w') as fp:
    fp.write(content)
