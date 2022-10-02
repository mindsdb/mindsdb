import os

def get_certs():
    certs_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "mysql")
    certs = {}
    for cert_key, fname in [("ssl_ca", "ca.pem"), ("ssl_cert", "client-cert.pem"), ("ssl_key", "client-key.pem")]:
        cert_file = os.path.join(certs_dir, fname)
        with open(cert_file, 'rb') as f:
            cert  = f.read()
            certs[cert_key] = cert
    return certs

certs = get_certs()
print(certs)
