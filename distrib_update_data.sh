PROJECT_DIRPATH="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

docker run \
    --rm \
    --workdir='/usr/src/myapp' \
    -v "${PROJECT_DIRPATH}:/usr/src/myapp" \
    ubuntu:18.04 bash -c "
      apt-get update;
      apt-get install -y build-essential python3.8 python3.8-venv python3-pip python3-wheel python3.8-dev;
      python3.8 -m venv .venv
      source .venv/bin/activate
        pip3 install wheel;
        pip3 install -r requirements.txt;
        pip3 install pyinstaller;
        pyinstaller update_data.py \
        --clean \
        --name update_data \
        --distpath=dist/linux/ \
        --onefile -y;
        chown -R ${UID} dist; "