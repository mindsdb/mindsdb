about = {}
with open("mindsdb/__about__.py") as fp:
    exec(fp.read(), about)
version = about['__version__']

with open('distributions/docker/Dockerfile', 'r') as fp:
    content = fp.read()
    content = content.replace('$version', version)

with open('distributions/docker/Dockerfile', 'w') as fp:
    fp.write(content)
