#!usr/bin/python

import os
import subprocess

def main():
    subprocess.check_call(["mkdir", "doc"])
    
    # documentation
    subprocess.check_call(["sphinx-build", "source", "doc"])

    
    subprocess.check_call(["ssh", "hitc5", "-o", "KbdInteractiveAuthentication=no", "-o", "ChallengeResponseAuthentication=no",
                           "rm", "-r", "-v", "-f", "/var/www/localhost/htdocs/build/pyharvester/doc"])
    subprocess.check_call(["scp", "-o", "KbdInteractiveAuthentication=no", "-o", "ChallengeResponseAuthentication=no",
                           "-r", "doc", "hitc5:/var/www/localhost/htdocs/build/pyharvester/"])
    
    subprocess.check_call(["rm", "-r", "v", "-f", "doc"])

if __name__ == '__main__':
    main()
