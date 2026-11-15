import importlib.util
import pathlib
import tempfile
import unittest
spec=importlib.util.spec_from_file_location('worker',pathlib.Path(__file__).with_name('hermes_bridge_worker.py'))
w=importlib.util.module_from_spec(spec);spec.loader.exec_module(w)
class PolicyTests(unittest.TestCase):
 def setUp(self):
  self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
  w.ROOT=pathlib.Path(self.tmp.name)
  self.repo=w.ROOT/'.raytsystem/workspaces/demo/repo';self.repo.mkdir(parents=True)
  self.request=dict(role='builder',mode='staging_only',workspace='/data/workspace/.raytsystem/workspaces/demo/repo',prompt='synthetic test')
 def test_valid(self):self.assertEqual(w.validate(self.request),self.repo)
 def test_roles_enforce_modes(self):
  for role in w.ROLE_SET-{'builder'}:
   with self.assertRaises(ValueError):w.validate(dict(self.request,role=role))
   self.assertEqual(w.validate(dict(self.request,role=role,mode='read_only')),self.repo)
 def test_paths_denied(self):
  for path in ['/data/workspace','/etc','/data/workspace/.raytsystem/workspaces/../repo','/data/workspace/.raytsystem/workspaces/no/repo']:
   with self.assertRaises(ValueError):w.validate(dict(self.request,workspace=path))
 def test_symlink_denied(self):
  (self.repo.parent/'link').symlink_to(self.repo,target_is_directory=True)
  with self.assertRaises(ValueError):w.validate(dict(self.request,workspace='/data/workspace/.raytsystem/workspaces/demo/link'))
 def test_invalid_inputs(self):
  for patch in [dict(role='admin'),dict(prompt=''),dict(prompt='a'*1048577),dict(prompt=23),dict(mode='read_only')]:
   with self.assertRaises(ValueError):w.validate(dict(self.request,**patch))
if __name__=='__main__':unittest.main()
