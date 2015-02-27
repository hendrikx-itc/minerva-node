/home/vagrant/bin:
  file.recurse:
    - source: salt://resources/commands/
    - file_mode: 755
    - group: vagrant
    - user: vagrant
    - makedirs: True
