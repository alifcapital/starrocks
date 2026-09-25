#!/usr/bin/env python3
"""Pin Docker Hub inputs and build the Ubuntu 24.04 development image."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import urllib.parse
import urllib.request

HERE = Path(__file__).resolve().parent
LOCK = HERE / "images.lock.json"


def resolve(image):
    repository, tag = image.rsplit(":", 1)
    query = urllib.parse.urlencode({"service": "registry.docker.io", "scope": f"repository:{repository}:pull"})
    with urllib.request.urlopen(f"https://auth.docker.io/token?{query}", timeout=60) as response:
        token = json.load(response)["token"]
    request = urllib.request.Request(
        f"https://registry-1.docker.io/v2/{repository}/manifests/{tag}",
        headers={"Authorization": f"Bearer {token}", "Accept": ", ".join([
            "application/vnd.oci.image.index.v1+json", "application/vnd.docker.distribution.manifest.list.v2+json",
            "application/vnd.oci.image.manifest.v1+json", "application/vnd.docker.distribution.manifest.v2+json",
        ])},
    )
    with urllib.request.urlopen(request, timeout=60) as response:
        data = response.read()
        digest = "sha256:" + hashlib.sha256(data).hexdigest()
        if response.headers.get("Docker-Content-Digest", digest) != digest:
            raise RuntimeError("Registry manifest digest mismatch")
    manifest = json.loads(data)
    platforms = [m["platform"] for m in manifest.get("manifests", []) if m.get("platform", {}).get("os") == "linux"]
    return {"tag": image, "image": f"{repository}@{digest}", "platforms": platforms}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    refresh = commands.add_parser("refresh", help="Resolve upstream tags and update images.lock.json")
    refresh.add_argument("--upstream", default="starrocks/dev-env-ubuntu:4.1-latest")
    refresh.add_argument("--toolchain", default="starrocks/toolchains-ubuntu:main-20260707")
    build = commands.add_parser("build", help="Build only from the committed image digests")
    build.add_argument("--tag", required=True)
    build.add_argument("--platform", default="linux/amd64")
    build.add_argument("--push", action="store_true")
    args = parser.parse_args()
    if args.command == "refresh":
        lock = {"upstream": resolve(args.upstream), "toolchain": resolve(args.toolchain)}
        LOCK.write_text(json.dumps(lock, indent=2) + "\n")
        print(LOCK.read_text(), end="")
        return
    lock = json.loads(LOCK.read_text())
    revision = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=HERE, text=True).strip()
    subprocess.run([
        "docker", "buildx", "build", "--pull", "--progress=plain", "--platform", args.platform,
        "--push" if args.push else "--load", "--tag", args.tag,
        "--build-arg", "UPSTREAM_IMAGE=" + lock["upstream"]["image"],
        "--build-arg", "TOOLCHAIN_IMAGE=" + lock["toolchain"]["image"],
        "--build-arg", "SOURCE_REVISION=" + revision,
        "--file", str(HERE / "dev-env.Dockerfile"), str(HERE),
    ], check=True)


if __name__ == "__main__":
    main()
