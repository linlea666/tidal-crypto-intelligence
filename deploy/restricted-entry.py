#!/usr/bin/python3
"""SSH forced command; no interactive shell, forwarding or arbitrary arguments."""
import os,re,subprocess,sys
command=os.environ.get('SSH_ORIGINAL_COMMAND','')
match=re.fullmatch(r'deploy (sha256:[0-9a-f]{64}) ([0-9a-f]{40}) (v[0-9]+\.[0-9]+\.[0-9]+)',command)
if not match:
    print('Only a stable Tidal release deployment is permitted.',file=sys.stderr)
    sys.exit(64)
sys.exit(subprocess.call(['/usr/bin/sudo','-n','/opt/tidal/bootstrap/deploy.sh']+list(match.groups())))
