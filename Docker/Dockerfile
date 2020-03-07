# Docker image spring/computational-env:2
FROM conda/miniconda3
USER root
RUN apt-get update \
&& apt-get install -y \
    build-essential \
    git \
    libspatialindex-c4v5 \
    mercurial \
&& rm -rf /var/lib/apt/lists/*

SHELL ["/bin/bash", "-c"]
RUN conda create -y --name py38 python=3.8 && conda clean -a -y
RUN conda run -v -n py38 conda install -c conda-forge gdal=3.0.2
RUN conda run -v -n py38 pip install --no-cache-dir \
    cython \
    ecoshard==0.3.3 \
    pytest \
    pytest-cov \
    matplotlib \
    mock \
    numpy \
    retrying \
    rtree \
    scipy \
    setuptools-scm \
    shapely \
    sympy \
    taskgraph==0.9.0 \
    pygeoprocessing==1.9.2 \
    && conda clean -a -y
RUN conda init bash && echo "source activate py38" > ~/.bashrc

WORKDIR /var/workspace
ENTRYPOINT ["/bin/bash", "-i", "execute.sh"]
CMD []
