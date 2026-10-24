"""Initialize business runtime metadata while using source checkouts directly."""
import os
from pathlib import Path
import proto


def initialize(config):
    root=Path(config['workspace']);root.mkdir(parents=True,exist_ok=True)
    for relative,target in config.get('workspaceLinks',{}).items():
        path=root/relative;path.parent.mkdir(parents=True,exist_ok=True)
        target=Path(target)
        if path.is_symlink() and path.resolve()==target.resolve():continue
        if path.exists() and not path.is_symlink():raise ValueError('workspace link occupied: '+str(path))
        temporary=path.with_name('.'+path.name+'.link')
        temporary.unlink(missing_ok=True);temporary.symlink_to(target,target_is_directory=target.is_dir());os.replace(temporary,path)
    if 'executionNodes' in config:
        proto.atomic_write_json(str(root/'run/execution.json'),dict(nodes=config['executionNodes'],runtimeSource=config['runtimeSource'],runtimeRoot=config['runtimeRoot']))
