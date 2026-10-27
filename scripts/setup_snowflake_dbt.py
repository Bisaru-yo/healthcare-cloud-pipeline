"""Generate an encrypted local key and SQL to configure the dbt service user."""
import argparse
import base64
import getpass
import hashlib
import json
import os
from pathlib import Path

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

ROOT = Path(__file__).resolve().parents[1]
SECRET_ENV = 'DBT_ENV_SECRET_SNOWFLAKE_KEY_PASSPHRASE'


def prepare(key_path, passphrase, project=ROOT):
    key_path, project = Path(key_path).expanduser().resolve(), Path(project).resolve()
    if not passphrase:
        raise ValueError('A nonempty key passphrase is required')
    password = passphrase.encode('utf-8')
    if key_path.exists():
        key = serialization.load_pem_private_key(key_path.read_bytes(), password=password)
        if not isinstance(key, rsa.RSAPrivateKey) or key.key_size < 2048:
            raise ValueError('The existing key must be RSA with at least 2048 bits')
    else:
        key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        payload = key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
                                    serialization.BestAvailableEncryption(password))
        key_path.parent.mkdir(parents=True, exist_ok=True)
        # Exclusive creation prevents accidentally replacing an existing key.
        descriptor = os.open(key_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, 'wb') as handle:
            handle.write(payload)
    public_der = key.public_key().public_bytes(serialization.Encoding.DER,
                                               serialization.PublicFormat.SubjectPublicKeyInfo)
    public_body = base64.b64encode(public_der).decode('ascii')
    template = (project / 'infra/snowflake-dbt-user.sql.template').read_text()
    if template.count('__PUBLIC_KEY__') != 1:
        raise ValueError('Expected exactly one public-key placeholder')
    output = project / 'local/setup_dbt_user.sql'
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(template.replace('__PUBLIC_KEY__', public_body), encoding='utf-8')
    profile = project / 'dbt/profiles.yml'
    if not profile.exists():
        profile.write_text((project / 'dbt/profiles.yml.example').read_text(), encoding='utf-8')
    # Record only a local path, never the passphrase or private-key material.
    config = project / 'local/dbt_key_location.json'
    config.write_text(json.dumps({'private_key_path':key_path.as_posix()}, indent=2)+'\n')
    return {'status':'prepared', 'setup_sql':str(output), 'profile':str(profile),
            'public_key_fingerprint':'SHA256:'+base64.b64encode(hashlib.sha256(public_der).digest()).decode('ascii')}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--key-path', default=str(Path.home()/'.healthcare-cloud/snowflake_dbt_key.p8'))
    args = parser.parse_args()
    existing = Path(args.key_path).expanduser().exists()
    passphrase = os.environ.get(SECRET_ENV)
    if not passphrase:
        passphrase = getpass.getpass('Existing key passphrase: ' if existing else 'Choose a key passphrase (keep it private): ')
        if not existing and passphrase != getpass.getpass('Repeat the key passphrase: '):
            raise SystemExit('Passphrases differ; no key was written.')
    try:
        print(json.dumps(prepare(args.key_path, passphrase)))
    except Exception as exc:
        raise SystemExit('Local key setup failed ('+type(exc).__name__+'). Check the existing key passphrase and paths.') from None


if __name__ == '__main__':
    main()
