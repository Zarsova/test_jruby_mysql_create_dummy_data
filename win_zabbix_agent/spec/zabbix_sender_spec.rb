$:.unshift File.join(File.dirname(__FILE__), 'lib')

require 'win_zabbix_agent'
require 'logger'

describe WinZabbixAgent::ZabbixSender do
  before do
    @logger = Logger::new(STDERR)
    @logger.level = Logger::FATAL
  end

  let(:sender) { described_class.new(@logger) }

  describe "with new" do
    context "initialized with bad zabbix_agent.conf path" do
      status, out, err = described_class.new(@logger, 'bad zabbix_agent.conf path').send('test', 1)
      status.should == 1
      subject { out }
      it { should =~ /^Sending failed\. Use option -vv for more detailed output\./ }
    end
    context "initialized with bad zabbix_sender path" do
      subject {
        proc {
          described_class.new(@logger, nil, 'bad zabbix_sender path').send('test', 1)
        }
      }
      it { should raise_error(Errno::ENOENT) }
    end
    describe "#send" do
      it 'return status is 0' do
        status, out, err = sender.send('test', 1)
        status.should == 0
      end
    end
  end

  describe ".send" do

    context "initialized with bad zabbix_sender path" do
      subject {
        proc {
          described_class.send(@logger, nil, 'bad zabbix_sender path') do
            multi_send 'test', 1
          end
        }
      }
      it { should raise_error(Errno::ENOENT) }
    end

    context "initialized with bad zabbix_agent.conf path" do
      subject {
        proc {
          described_class.send(@logger, 'bad zabbix_agent.conf path') do
            multi_send 'test', 1
          end
        }
      }
      it { should raise_error(StandardError) }
    end

    context "ブロックを渡す場合" do
      it 'アイテム複数送信する' do
        described_class.send(@logger) do |sender|
          sender.multi_send 'key', 1
        end
      end
    end

    context "ブロックを渡さない場合" do
      subject {
        proc { described_class.send(@logger) }
      }
      it { should raise_error(ArgumentError) }
    end
  end
end