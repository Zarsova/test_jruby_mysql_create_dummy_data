# -*- coding: utf-8 -*-
#!/usr/bin/env ruby

def error_proc
  lambda do |msg|
    $stderr.puts(msg)
    exit 1
  end
end