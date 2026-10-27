"""Task-boundary and remote-integrity regressions; no cloud access."""
from contextlib import closing
import io
import json
from pathlib import Path
import shutil
import unittest
from unittest.mock import patch
import test_pipeline as fixtures
from test_export_publish import FakeS3
from pipeline.export import export_snapshot
from pipeline.publish import publish_batch
from pipeline.quality import QualityError,read_and_validate
from pipeline.storage import connect,load_snapshot
from pipeline import orchestrate as flow
from pipeline.snowflake_load import render_load_sql,verify_remote_batch,validate_load_metrics
KEY='af_'+'a'*24


class ReadableS3(FakeS3):
    def get_object(self,**kwargs):return {'Body':io.BytesIO(self.objects[kwargs['Key']]['Body'])}


class OrchestrationTests(unittest.TestCase):
    def setUp(self):
        f=fixtures.PipelineTests();f.setUp();self.addCleanup(f.doCleanups)
        self.root=f.root;self.fixture=f
        (self.root/'data').mkdir();shutil.copytree(f.source,self.root/'data/source')
        data,_=read_and_validate(f.source)
        db=self.root/'source.db'
        with closing(connect(str(db))) as connection:load_snapshot(connection,data)
        self.batch,self.manifest=export_snapshot(str(db),'sqlite',self.root/'data/batches',KEY)
        self.client=ReadableS3();publish_batch(self.batch,'bishal-healthcare-pipeline-bisar01',client=self.client)
        self.folder=self.root/'evidence/airflow'/KEY
        self.config={'database_url':'postgresql://user:sensitive-password@invalid/source','key_passphrase':'sensitive-passphrase','aws_profile':'healthcare-cloud','aws_region':'us-east-2'}

    def evidence(self,name,**extra):
        value={'run_key':KEY,'step':name,'status':'passed',**extra}
        flow.atomic_json(self.folder/(name+'.json'),value)
        return value

    def test_remote_files_are_rehashed_and_rows_verified(self):
        self.assertEqual(verify_remote_batch(self.client,KEY,self.manifest['content_sha256']),self.manifest)

    def test_remote_tampering_prevents_database_load(self):
        key='healthcare/'+KEY+'/encounters.csv';self.client.objects[key]['Body']+=b'changed'
        with self.assertRaises(ValueError):verify_remote_batch(self.client,KEY,self.manifest['content_sha256'])

    def test_remote_manifest_from_different_batch_is_rejected(self):
        with self.assertRaises(ValueError):verify_remote_batch(self.client,KEY,'0'*64)

    def test_sql_uses_validated_new_batch_and_nonadmin_role(self):
        sql=render_load_sql(self.manifest)
        self.assertIn('USE ROLE HEALTHCARE_LOADER;',sql);self.assertNotIn('USE ROLE ACCOUNTADMIN;',sql)
        self.assertNotIn('20261026T235514_a3bfed17dc09',sql);self.assertIn('@HEALTHCARE_S3_STAGE/'+KEY+'/',sql)
        with self.assertRaises(ValueError):render_load_sql({**self.manifest,'batch_id':"x'; DROP DATABASE x;"})

    def test_sql_template_drift_is_not_silently_accepted(self):
        with self.assertRaises(ValueError):render_load_sql(self.manifest,template='SELECT 1;')

    def test_reconciliation_checks_snapshot_keys_not_cumulative_target_count(self):
        rows=[{'table_name':t,'status':'passed','source_rows':m['row_count'],'reconciled_rows':m['row_count'],'inserted':0,'updated':0,'unchanged':m['row_count'],'target_total_rows':m['row_count']+5} for t,m in self.manifest['tables'].items()]
        self.assertEqual(validate_load_metrics(rows,self.manifest),rows)
        rows[0]['reconciled_rows']=0
        with self.assertRaises(ValueError):validate_load_metrics(rows,self.manifest)

    def test_export_retry_reuses_original_verified_batch(self):
        self.evidence('validate_source');before=(self.batch/'manifest.json').read_bytes()
        with patch.object(flow,'export_snapshot',side_effect=AssertionError('must not export again')):
            result=flow.execute_step('export_snapshot',KEY,self.config,self.root)
        self.assertTrue(result['reused_local_batch']);self.assertEqual(before,(self.batch/'manifest.json').read_bytes())

    def test_publish_retry_reuses_all_four_objects(self):
        self.evidence('export_snapshot',content_sha256=self.manifest['content_sha256'])
        with patch.object(flow,'s3_client',return_value=self.client):result=flow.execute_step('publish_s3',KEY,self.config,self.root)
        self.assertEqual(result['uploaded'],0);self.assertEqual(result['reused'],4)

    def test_source_quality_failure_keeps_evidence_and_blocks_next_stage(self):
        self.fixture.data['encounters'][0]['patient']=fixtures.PAYER;self.fixture.write()
        shutil.rmtree(self.root/'data/source');shutil.copytree(self.fixture.source,self.root/'data/source')
        with self.assertRaises(QualityError):flow.execute_step('validate_source',KEY,self.config,self.root)
        failure=json.loads((self.folder/'validate_source.json').read_text());self.assertEqual(failure['error_type'],'QualityError')
        with patch.object(flow,'export_snapshot',side_effect=AssertionError('must not export')):
            with self.assertRaises(ValueError):flow.execute_step('export_snapshot',KEY,self.config,self.root)
        for path in self.folder.rglob('*.json'):
            self.assertNotIn('sensitive-password',path.read_text());self.assertNotIn('sensitive-passphrase',path.read_text())

    def test_final_success_requires_every_stage_to_pass(self):
        self.evidence('verify_reports')
        with self.assertRaises(FileNotFoundError):flow.execute_step('finalize',KEY,self.config,self.root)
        self.assertEqual(json.loads((self.folder/'finalize.json').read_text())['status'],'failed')

    def test_different_run_evidence_cannot_satisfy_dependency(self):
        self.evidence('validate_source',run_key='af_'+'b'*24)
        with self.assertRaises(ValueError):flow.execute_step('export_snapshot',KEY,self.config,self.root)

    def test_stream_execution_passes_filelike_sql_and_closes_all_cursors(self):
        from decimal import Decimal
        from pipeline.snowflake_load import execute_load
        names=['run_id','status','table_name','inserted','updated','unchanged','source_rows','reconciled_rows','target_total_rows']
        rows=[('run','passed',t,Decimal(0),Decimal(0),Decimal(m['row_count']),Decimal(m['row_count']),Decimal(m['row_count']),Decimal(m['row_count']+5)) for t,m in self.manifest['tables'].items()]
        class Cursor:
            def __init__(self,columns,data):self.description=[(n,) for n in columns];self.data=data;self.closed=False
            def fetchall(self):return self.data
            def close(self):self.closed=True
        cursors=[Cursor(['status'],[('DDL completed',)]),Cursor(names,rows)]
        class Connection:
            def execute_stream(self,stream):
                sql=stream.read()
                assert 'USE ROLE HEALTHCARE_LOADER;' in sql
                return iter(cursors)
        result=execute_load(Connection(),self.manifest)
        self.assertTrue(all(c.closed for c in cursors))
        self.assertTrue(all(isinstance(r['source_rows'],int) for r in result))

    def test_missing_final_load_metrics_is_not_success(self):
        from pipeline.snowflake_load import execute_load
        class Connection:
            def execute_stream(self,stream):return iter([])
        with self.assertRaises(ValueError):execute_load(Connection(),self.manifest)
