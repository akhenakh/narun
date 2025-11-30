```sh
    
podman build -t narun-test -f Dockerfile.test .

  
```

```sh
podman run --rm -it \
  --privileged \
  -v $(pwd):/src \
  narun-test
```
