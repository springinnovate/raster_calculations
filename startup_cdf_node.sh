#!/bin/bash
apt-get update
apt-get install \
    apt-transport-https \
    ca-certificates \
    curl \
    gnupg2 \
    emacs \
    software-properties-common -y
curl -fsSL https://download.docker.com/linux/debian/gpg | apt-key add -
add-apt-repository \
   "deb [arch=amd64] https://download.docker.com/linux/debian \
   $(lsb_release -cs) \
   stable"
apt-get update
apt-get install docker-ce docker-ce-cli containerd.io -y
git clone https://github.com/therealspring/raster_calculations.git /usr/local/raster_calculations
cd /usr/local/raster_calculations
docker run --rm -it -v `pwd`:/var/workspace therealspring/computational-env:4 cumulative_density_function_per_country.py > log.txt
