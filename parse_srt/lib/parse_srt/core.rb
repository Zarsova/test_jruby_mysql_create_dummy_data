#!/usr/bin/env ruby
# -*- coding: utf-8 -*-

module ParseSrt
  module Core
    def inits
      @inits ||= []
    end

    def commands
      @commands ||= []
    end

    def init(&block)
      inits << block
    end

    def mutex
      @mutex ||= Mutex.new
    end

    def sync(&block)
      mutex.synchronize do
        block.call
      end
    end

    def command(pattern, options = {}, &block)
      if block
        command_name = ":#{pattern}"
        if block.arity > 0
          pattern = %r|^#{command_name}\s+(.*)$|
        else
          pattern = %r|^#{command_name}$|
        end
        commands << {:pattern => pattern, :block => block}
      else
        commands.detect { |c| c[:pattern] =~ pattern }
      end
    end

    def input(text)
      system("cls")
      if command = command(text)
        command[:block].call()
      end
    end

    def start(options = {})
      _start = Time.now
      inits.each { |block| class_eval(&block) }
      inits.clear
      begin
        EM.run do
          Readline.basic_word_break_characters = " \t\n\"\\'`$><=;|&{(@"
          while buf = Readline.readline("> ", true)
            sync {
              input(buf.strip)
            }
          end
          EM.stop_event_loop
        end
      ensure
        puts "Seconds spent #{Time.now - _start}"
      end
      exit 0
    end

    def stop
      EventMachine.stop_event_loop
      exit 0
    end
  end
  extend Core
end
