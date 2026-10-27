"""Regression checks for local credential handling; no Snowflake access."""
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

HAS_CRYPTO = importlib.util.find_spec('cryptography') is not None
if HAS_CRYPTO:
    from cryptography.hazmat.primitives import serialization
    spec = importlib.util.spec_from_file_location('dbt_setup',Path(__file__).resolve().parents[1]/'scripts/setup_snowflake_dbt.py')
    setup = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(setup)


@unittest.skipUnless(HAS_CRYPTO,'Optional dbt dependency cryptography is not installed')
class DbtKeySetupTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.project = Path(self.temp.name)/'project'
        (self.project/'infra').mkdir(parents=True)
        (self.project/'dbt').mkdir()
        original=Path(__file__).resolve().parents[1]
        for rel in ['infra/snowflake-dbt-user.sql.template','dbt/profiles.yml.example']:
            (self.project/rel).write_bytes((original/rel).read_bytes())
        self.key = Path(self.temp.name)/'private/key.p8'
        self.password = 'test-only-passphrase'

    def test_encrypted_private_key_stays_outside_project_and_passphrase_is_not_saved(self):
        result=setup.prepare(self.key,self.password,self.project)
        key=serialization.load_pem_private_key(self.key.read_bytes(),password=self.password.encode())
        self.assertEqual(key.key_size,2048)
        with self.assertRaises(TypeError):
            serialization.load_pem_private_key(self.key.read_bytes(),password=None)
        for path in self.project.rglob('*'):
            if path.is_file():
                self.assertNotIn(self.password,path.read_text())
                self.assertNotIn('BEGIN ENCRYPTED PRIVATE KEY',path.read_text())
        self.assertTrue(result['public_key_fingerprint'].startswith('SHA256:'))
        self.assertNotIn('__PUBLIC_KEY__',(self.project/'local/setup_dbt_user.sql').read_text())

    def test_rerun_reuses_key_and_preserves_existing_profile(self):
        setup.prepare(self.key,self.password,self.project)
        before=self.key.read_bytes()
        (self.project/'dbt/profiles.yml').write_text('existing profile\n')
        setup.prepare(self.key,self.password,self.project)
        self.assertEqual(before,self.key.read_bytes())
        self.assertEqual((self.project/'dbt/profiles.yml').read_text(),'existing profile\n')

    def test_wrong_passphrase_does_not_replace_existing_key_or_setup_sql(self):
        setup.prepare(self.key,self.password,self.project)
        before=self.key.read_bytes()
        sql=(self.project/'local/setup_dbt_user.sql').read_bytes()
        with self.assertRaises(ValueError):
            setup.prepare(self.key,'wrong-passphrase',self.project)
        self.assertEqual(before,self.key.read_bytes())
        self.assertEqual(sql,(self.project/'local/setup_dbt_user.sql').read_bytes())
