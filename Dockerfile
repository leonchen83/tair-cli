FROM maven:3 AS builder

WORKDIR /app
COPY . /app
RUN mvn clean install -Dmaven.test.skip=true

FROM taobeier/openjdk
COPY --from=builder /app/target/tair-cli-release.zip /tmp/tair-cli-release.zip
WORKDIR /app
# because of the cli has set shebang
RUN unzip -o /tmp/tair-cli-release.zip \
        && apk add --no-cache bash \
        && rm -f /tmp/tair-cli-release.zip \
        && ln -s /app/tair-cli/bin/tair-cli /usr/local/bin/tair-cli \
        && ln -s /app/tair-cli/bin/tair-monitor /usr/local/bin/tair-monitor

WORKDIR /app/tair-cli