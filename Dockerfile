FROM nimlang/nim:2.2.8

WORKDIR /app

COPY gloci.nimble ./
COPY src ./src

RUN nim c -d:release --opt:speed --out:/usr/local/bin/gloci src/gloci.nim

EXPOSE 8080
ENV GLOCI_PORT=8080

CMD ["gloci"]
