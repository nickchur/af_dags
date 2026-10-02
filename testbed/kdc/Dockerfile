# KDC (MIT krb5) тестового стенда: realm STAND.TEST, keytab'ы — в смонтированный /keytabs.
# 2026-10-02 10:12 MSK · v1.0 · Nick Churkin · NSChurkin@sber.ru
FROM debian:bookworm-slim
RUN apt-get update \
 && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends krb5-kdc krb5-admin-server \
 && rm -rf /var/lib/apt/lists/*
COPY kdc.conf /etc/krb5kdc/kdc.conf
COPY krb5-kdc.conf /etc/krb5.conf
COPY entrypoint.sh /entrypoint.sh
EXPOSE 88/tcp 88/udp
CMD ["/bin/sh", "/entrypoint.sh"]
