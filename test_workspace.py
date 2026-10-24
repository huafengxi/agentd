import json
from pathlib import Path
import tempfile
import unittest
from workspace import initialize

class Workspace(unittest.TestCase):
    def test_fresh_runtime_creates_protocol_directory_without_copying_sources(self):
        with tempfile.TemporaryDirectory(prefix='agent-workspace-') as temp:
            base=Path(temp);source=base/'code';source.mkdir();(source/'tool.py').write_text('source')
            root=base/'data/workspace';runtime=base/'runtime'
            config=dict(workspace=str(root),runtimeRoot=str(runtime),runtimeSource=str(source),executionNodes={},workspaceLinks={'agents':str(runtime/'shared/agents'),'agentd':str(source)})
            initialize(config)
            self.assertTrue((root/'agents').is_dir());self.assertTrue((root/'agentd').is_symlink())
            task=root/'agents/task/example/spec.json';task.parent.mkdir(parents=True);task.write_text('business history')
            initialize(config)
            self.assertEqual(task.read_text(),'business history')
            self.assertEqual(json.loads((root/'run/execution.json').read_text())['runtimeRoot'],str(runtime))
            self.assertEqual(list(source.iterdir()),[source/'tool.py'])

if __name__=='__main__':unittest.main()
