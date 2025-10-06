import os
import ssl
import socket
from logging import Logger
# Your confirmed paths (../certs/ from python-backend/)
CERT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'certs'))
cafile = os.path.join(CERT_DIR, 'AmazonRootCA1.pem')
certfile = os.path.join(CERT_DIR, '8ba3789f5cbeb11db4ffe8f3a8223725e7242e6417aade8ac33929221b997a92-certificate.pem.crt')
keyfile = os.path.join(CERT_DIR, '8ba3789f5cbeb11db4ffe8f3a8223725e7242e6417aade8ac33929221b997a92-privat.key')

print(f"Cert paths:\nCA={cafile} (exists: {os.path.exists(cafile)})\nCert={certfile} (exists: {os.path.exists(certfile)})\nKey={keyfile} (exists: {os.path.exists(keyfile)})")
print(f"CERT_DIR={CERT_DIR}")

if not all([os.path.exists(f) for f in [cafile, certfile, keyfile]]):
    print("ERROR: Missing files. Double-check dir.")
    exit(1)

# Enable SSL debugging (optional, comment out if too verbose)
logging.getLogger('ssl').setLevel(logging.DEBUG)
import logging
logging.basicConfig(level=logging.DEBUG)

context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
context.load_verify_locations(cafile=cafile)
try:
    context.load_cert_chain(certfile=certfile, keyfile=keyfile)
    print("\nSUCCESS: Cert chain loaded (key/cert match OK).")
except Exception as e:
    print(f"\nERROR loading certs: {e}")
    import traceback
    traceback.print_exc()
    exit(1)

sock = context.wrap_socket(socket.socket(socket.AF_INET), server_side=False)
sock.settimeout(30)  # 30s for handshake
try:
    print("\nAttempting TLS connect to AWS IoT (port 8883)...")
    sock.connect(("a1dghi6and062t-ats.iot.us-east-1.amazonaws.com", 8883))
    print("SUCCESS: TLS handshake complete! (Certs valid, network/port OK). MQTT should connect now.")
    # If here, try manager.py
except ssl.SSLError as e:
    print(f"SSL/TLS ERROR: {e} (Cert rejected by AWS—check key/cert pair, attachment, or policy. Redownload if needed.)")
    print("AWS Logs: Check IoT > Logs > CloudWatch for 'UnauthorizedClient' or 'InvalidCert'.")
except socket.timeout:
    print("TIMEOUT: Connection timed out (Firewall/VPN blocking port 8883, or network slow). Disable antivirus/firewall temporarily.")
except socket.gaierror as e:
    print(f"DNS/Host ERROR: {e} (Bad endpoint? Confirm in AWS IoT > Settings > Custom endpoint).")
except Exception as e:
    print(f"OTHER ERROR: {e}")
    import traceback
    traceback.print_exc()
finally:
    sock.close()
