FROM 10.9.49.222:5000/chunyu_base:v5

MAINTAINER Li Yichao <liyichao.good@gmail.com>

COPY ./docker/sources.list /etc/apt/sources.list
RUN	apt-get update && \
	apt-get install -y --no-install-recommends \
	build-essential \
	libtool \
	automake

WORKDIR /kids

COPY . /kids
RUN ./autogen.sh && ./configure && make

EXPOSE :3388

CMD ["src/kids", "-c", "/kids/kids.conf"]

PUSH  10.9.49.222:5000/kids:v1

