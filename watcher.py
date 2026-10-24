#!/usr/bin/env python3
"""Deliver agent notifications from rund facts; never execute or mirror state."""
import argparse
import json
from pathlib import Path
import proto
from notifications import Notifications, acquire_single_instance_lock, setup_logging

class ProtocolWatcher(Notifications):
    def __init__(self, config):
        self.config = config
        self.ownerlock, heartbeat = acquire_single_instance_lock(config['workspace'], config['host'])
        super().__init__(config['workspace'], config['host'], config.get('aliases', ''),
                         config.get('interval', .5), lockfile=heartbeat, lock_host=config['host'])

    def service(self, participant):
        spec = self.read_spec(participant)
        if not spec or not proto.host_matches(spec, self.local_ids): return
        state = self.read_execution(participant)
        if state and not proto.life_terminal(state): self._seen_nonterminal.add(participant)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', required=True)
    parser.add_argument('--log-file')
    args = parser.parse_args()
    setup_logging(args.log_file, 'INFO')
    config=json.loads(Path(args.config).read_text())
    from workspace import initialize
    initialize(config)
    watcher = ProtocolWatcher(config)
    try: watcher.run()
    finally: watcher.ownerlock.close()

if __name__ == '__main__': main()
