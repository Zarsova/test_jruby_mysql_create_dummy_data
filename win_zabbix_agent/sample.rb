# -*- coding: utf-8 -*-
$:.unshift File.join(File.dirname(__FILE__), 'lib')

require 'win_zabbix_agent'

s = WinZabbixAgent::ZabbixSender.new()
s.send 'key1', 1

WinZabbixAgent::ZabbixSender.send do
  multi_send 'key1', 1
  multi_send 'key2', 2
  multi_send 'key3', 3
end
\
WinZabbixAgent::ZabbixSender.send do
  strict true
  multi_send 'key1', 1
  multi_send 'key2', 2
  multi_send 'key3', 3
end
