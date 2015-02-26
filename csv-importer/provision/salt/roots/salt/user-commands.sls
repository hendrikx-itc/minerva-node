/home/vagrant/bin:
  file:
    - recurse
    - source: salt://resources/user_commands
    - user: vagrant
    - group: vagrant
    - file_mode: 755
    - makedirs: True
