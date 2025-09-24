FROM  amazonlinux:2

RUN yum -y update
RUN yum -y install gcc make openssl-devel bzip2-devel libffi-devel zlib-devel git tar libyaml-devel zip
RUN curl -sSL https://cache.ruby-lang.org/pub/ruby/3.2/ruby-3.2.0.tar.gz | tar xz \
    && cd ruby-3.2.0 \
    && ./configure \
    && make \
    && make install \
    && cd .. \
    && gem install bundler

