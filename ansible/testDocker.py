import docker
client = docker.DockerClient(base_url="unix:///var/run/docker.sock")
try:
    print(client.version())
except Exception as e:
    print("Connection error:", e)

